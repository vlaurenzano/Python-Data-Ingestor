import unittest

from lib.ingest_data import *
from lib.services import *
import datetime


class TestIngestData(unittest.TestCase):

    def setUp(self):
        self.db = get_mongo_client().unit_test
        self.db.drop_collection('ingestions')
        self.db.drop_collection('facilities')
        self.i_collection = self.db.ingestions
        self.f_collection = self.db.facilities

    def test_get_last_collection(self):
        self.i_collection.insert_one({"test": "one"})
        self.i_collection.insert_one({"test": "two"})
        i = get_last_ingestion(self.i_collection)
        self.assertEquals(i["test"], "two")

    def test_insert_ingestion(self):
        dt = datetime.datetime.utcnow()
        insert_ingestion(self.i_collection, dt)
        i = get_last_ingestion(self.i_collection)
        self.assertEquals(i["last_modified"].strftime('%Y%m%d%H%m'), dt.strftime('%Y%m%d%H%m'))

    def test_generate_checksum(self):
        x = generate_checksum({"oi": "nnnn", "test":"ddd"})
        y = generate_checksum({"test":"ddd", "oi": "nnnn"})
        self.assertEquals(x, y)

    def test_ingest_data_inserts_unqiue_records(self):
        ingest_data(self.f_collection,[{"hello":"dfdf"}, {"other":"sdfsdf"}],'1')
        self.assertEquals(2, self.f_collection.count())
        ingest_data(self.f_collection,[{"hello":"dfdf"}, {"other":"sdfsdf"}],'1')
        self.assertEquals(2, self.f_collection.count())
        ingest_data(self.f_collection,[{"hello":"dfdf"}, {"other":"ssdfsdfsdfdf"}],'2')
        self.assertEquals(3, self.f_collection.count())
        ingest_data(self.f_collection,[{"hello1":"dfdf"}, {"other":"ssdfsdfsdfdf"}],'2')
        self.assertEquals(4, self.f_collection.count())

    def test_ingest_data_deactivates_old_records(self):
        ingest_data(self.f_collection,[{"hello":"dfdf"}, {"other":"sdfsdf"}],'1')
        ingest_data(self.f_collection,[{"hello2":"dfdf"}, {"other":"sdfsdf"}],'2')
        doc1 = self.f_collection.find_one()
        self.assertFalse(doc1['active'])

    def test_ingest_one_with_empty_collections(self):
        ingest_one(self.f_collection, self.i_collection, {"my_doc":"sdfasdasd"})
        self.assertEquals(self.i_collection.count(), 1)
        self.assertEquals(self.f_collection.count(), 1)

    def test_ingest_one_with_non_empty_ollections(self):
        insert_ingestion(self.i_collection, datetime.datetime.utcnow())
        ingest_data(self.f_collection,[{"my_doc":"sdfasdasd"}],'1')
        insert_ingestion(self.i_collection, datetime.datetime.utcnow())
        ingest_data(self.f_collection,[{"my_doc":"sdfasdfsdfsdasd"}],'2')
        i = get_last_ingestion(self.i_collection)
        ingest_one(self.f_collection, self.i_collection, {"my_doc":"sdfasdasd"})
        self.assertEquals(self.f_collection.count(), 2)
        c = self.f_collection.find_one()
        self.assertEquals(c['ingestion_id'] , i['_id'])


    def test_ingest_data_does_not_overwrite_data(self):
        ingest_data(self.f_collection,[{"my_doc":"sdfasdasd"}],'1')
        doc1 = self.f_collection.find_one()
        self.f_collection.update({'_id': doc1['_id']}, {"$set":{"my_data": 'test'}})
        ingest_data(self.f_collection,[{"my_doc":"sdfasdasd"}],'2')
        doc2 = self.f_collection.find_one()
        self.assertEquals(doc2['my_data'], 'test')
        self.assertEquals(doc2['ingestion_id'], '2')
        self.assertEquals(doc1['_id'], doc2['_id'])
        self.assertEquals(1, self.f_collection.count())

    def mock_get_data(self):
       return [{
            "name_2": "Visiting Nurse Bronx ACT Program",
            "name_1": "Visiting Nurse Service of New York",
            "flag_md": "1",
            "city": "BRONX",
            "latitude": "40.8166695637",
            "longitude": "-73.9199589703",
            "zip": "10451",
            "website": "http://www.vnsny.org/",
            "flag_mhf": "1",
            "street_1": "349 E. 149th Street",
            "phone": "718-742-5155",
            "flag_chld": "1",
            "flag_yad": "1"
        }
            , {
            "name_2": "VNS Children and Adolescent MH Clinic at FRIENDS",
            "name_1": "Visiting Nurse Service of New York",
            "flag_md": "1",
            "city": "BRONX",
            "latitude": "40.8111616432999",
            "longitude": "-73.9098036958999",
            "zip": "10455",
            "website": "http://www.vnsny.org/",
            "flag_pi": "1",
            "flag_mhf": "1",
            "street_1": "470 Jackson Avenue",
            "phone": "718-742-7000",
            "street_2": "Room 117",
            "flag_chld": "1",
            "flag_yad": "1"
        }
            , {
            "name_2": "Advanced Center for Psychotherapy Forest Hills",
            "name_1": "Advanced Center for Psychotherapy, Inc.",
            "flag_md": "1",
            "city": "QUEENS",
            "latitude": "40.7253089279999",
            "flag_mc": "1",
            "flag_snr": "1",
            "longitude": "-73.8494671755999",
            "zip": "11375",
            "website": "http://jamaicahospital.org/index.php/clinical-services/psychiatry/advanced-center-of-psychotherapy/",
            "flag_pi": "1",
            "flag_mhf": "1",
            "flag_saf": "1",
            "street_1": "103-26 68th Road",
            "phone": "718-261-3330",
            "flag_adlt": "1",
            "flag_chld": "1",
            "flag_yad": "1"
        }], datetime.datetime.utcnow()

    def test_run_ingestion(self):
        run_ingestion(self.f_collection, self.i_collection, self.mock_get_data)
        self.assertEquals(self.f_collection.count(), 3)

