"""Cold-start concurrency regression; no connection to Atlas."""
import os
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest.mock import MagicMock, patch

os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
import app.db.client as module


class ClientConcurrencyTests(unittest.TestCase):
    def test_concurrent_cold_start_constructs_one_pool(self):
        module._client = None
        barrier = Barrier(8)
        shared = MagicMock()

        def construct(*args, **kwargs):
            time.sleep(0.03)
            return shared

        def acquire(_):
            barrier.wait(timeout=5)
            return module.client()

        try:
            with patch.object(module, "MongoClient", side_effect=construct) as constructor:
                with ThreadPoolExecutor(max_workers=8) as executor:
                    handles = list(executor.map(acquire, range(8)))
                self.assertEqual(constructor.call_count, 1)
                self.assertTrue(all(handle is handles[0] for handle in handles))
        finally:
            module._client = None
