from . import context
from .base import DatabaseTestCase, STUDENTS_SCHEMA, create_test_student
from pydb.query import CreateTable, Insert, Select, From
from pydb.disk import HeapFile, DiskTable, DiskDatabase
import os
import shutil
import tempfile
import unittest


class HeapFileTestCase(unittest.TestCase):
    def test_heap_file_basics(self):
        fd, path = tempfile.mkstemp("heap_file_test")
        os.close(fd)

        records = [
            (0, "a", 0.0),
            (1, "b", 1.1),
            (2, "c", 2.2),
            (3, "d", 3.3),
        ]
        offsets = []
        with HeapFile.open(path) as f:
            for record in records:
                offsets.append(f.append(record))
            for record, offset in zip(records, offsets):
                self.assertEqual(f.get(offset), record)
            self.assertEqual(list(iter(f)), list(zip(offsets, records)))
        with HeapFile.open(path) as f:
            for record, offset in zip(records, offsets):
                self.assertEqual(f.get(offset), record)
            self.assertEqual(list(iter(f)), list(zip(offsets, records)))

            records.append((4, "e", 4.4))
            offsets.append(f.append(records[-1]))

            for record, offset in zip(records, offsets):
                self.assertEqual(f.get(offset), record)
            self.assertEqual(list(iter(f)), list(zip(offsets, records)))

    @unittest.skip
    def test_heap_file_corruption(self):
        pass


class DiskTableTestCase(unittest.TestCase):
    def test_disk_table(self):
        records = [
            (0, "a", 0.0),
            (1, "b", 1.1),
            (2, "c", 2.2),
        ]
        row_ids = []
        folder = tempfile.mkdtemp()
        with DiskTable.open(STUDENTS_SCHEMA, folder) as table:
            for record in records:
                row_ids.append(table.insert(record)[0])
            for row_id, record in zip(row_ids, records):
                self.assertEqual(table.get(row_id), record)
        with DiskTable.open(STUDENTS_SCHEMA, folder) as table:
            for row_id, record in zip(row_ids, records):
                self.assertEqual(table.get(row_id), record)
            for record, row in zip(records, table.rows()):
                self.assertEqual(record, row)


class DiskDatabaseTestCase(DatabaseTestCase, unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.mkdtemp()
        self.initdb(DiskDatabase.open(self.folder))

    def tearDown(self):
        error = None
        try:
            self.db.close()
        except Exception as err:
            error = err
        shutil.rmtree(self.folder)
        if error:
            raise error

    def test_open_close(self):
        students = [create_test_student(x) for x in range(10)]
        with DiskDatabase.open(self.folder) as db:
            db.exec(CreateTable(STUDENTS_SCHEMA))
            for student in students:
                db.exec(Insert("students", STUDENTS_SCHEMA.column_names(), student))
        with DiskDatabase.open(self.folder) as db:
            results = db.exec(Select(STUDENTS_SCHEMA.column_names(), From("students")))
            results = list(results)
            results.sort(key=lambda s: s[0])
            self.assertEqual(results, students)
