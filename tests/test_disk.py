from . import context
import unittest
from pydb.disk import *
import os
import tempfile


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
