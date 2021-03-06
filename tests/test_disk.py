from . import context
from .base import DatabaseTestCase, STUDENTS_SCHEMA, create_test_student
from pydb.query import CreateTable, Insert, Select, From
from pydb.disk import RowHeader, HeapFile, DiskTable, DiskDatabase
from io import BytesIO
import os
import shutil
import tempfile
import unittest


def test_row_header():
    h = RowHeader(size=4, tombstone=False)
    with BytesIO() as buf:
        h.write(buf)
        assert len(buf.getvalue()) == RowHeader.SIZE
        buf.seek(0)
        assert h == RowHeader.read(buf)


class HeapFileTestCase(unittest.TestCase):
    def test_heap_file(self):
        fd, path = tempfile.mkstemp("heap_file_test")
        os.close(fd)

        records = [
            (0, "a", 0.0),
            (1, "b", 1.1),
            (2, "c", 2.2),
            (3, "d", 3.3),
        ]
        offsets = []

        def check_contents(f: HeapFile):
            for record, offset in zip(records, offsets):
                self.assertEqual(f.get(offset), record)
            self.assertEqual(list(iter(f)), list(zip(offsets, records)))

        with HeapFile.open(path) as f:
            for record in records:
                offsets.append(f.append(record))
            check_contents(f)
        with HeapFile.open(path) as f:
            check_contents(f)

            records.append((4, "e", 4.4))
            offsets.append(f.append(records[-1]))

            check_contents(f)

            for idx in (-1, 0):
                f.remove(offsets[idx])
                with self.assertRaises(ValueError):
                    f.get(offsets[idx])
                offsets.pop(idx)
                records.pop(idx)

            check_contents(f)


class DiskTableTestCase(unittest.TestCase):
    def test_disk_table(self):
        records = [create_test_student(x) for x in range(10)]
        row_ids = []
        folder = tempfile.mkdtemp()

        def check_table(table):
            for row_id, record in zip(row_ids, records):
                self.assertEqual(table.get(row_id), record)
            for record, row in zip(records, table.rows()):
                self.assertEqual(record, row)

        with DiskTable.open(STUDENTS_SCHEMA, folder) as table:
            for record in records:
                row_ids.append(table.insert(record)[0])
            check_table(table)
        with DiskTable.open(STUDENTS_SCHEMA, folder) as table:
            check_table(table)
            for idx in (-1, 3, 0):
                table.delete(row_ids[idx])
                row_ids.pop(idx)
                records.pop(idx)
            check_table(table)


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

    def test_open_missing_parent_folder(self):
        path = os.path.join(self.folder, "foo", "bar")
        with self.assertRaises(ValueError):
            with DiskDatabase.open(path) as db:
                pass

    def test_open_creates_folder(self):
        path = os.path.join(self.folder, "foo")
        with DiskDatabase.open(path) as db:
            pass  # ok

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
