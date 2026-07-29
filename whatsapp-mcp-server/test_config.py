import importlib
import os
import unittest
from unittest import mock

import whatsapp


class EnvironmentConfigTest(unittest.TestCase):
    def tearDown(self):
        importlib.reload(whatsapp)

    def test_account_specific_environment(self):
        env = {
            "WHATSAPP_ACCOUNT_NAME": "Trainer Connect",
            "WHATSAPP_MESSAGES_DB_PATH": "/tmp/trainer-connect/messages.db",
            "WHATSAPP_API_BASE_URL": "http://127.0.0.1:8081/api/",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            module = importlib.reload(whatsapp)
            self.assertEqual(module.WHATSAPP_ACCOUNT_NAME, "Trainer Connect")
            self.assertEqual(
                module.MESSAGES_DB_PATH,
                "/tmp/trainer-connect/messages.db",
            )
            self.assertEqual(
                module.WHATSAPP_API_BASE_URL,
                "http://127.0.0.1:8081/api",
            )

    def test_account_info_uses_configured_bridge(self):
        response = mock.Mock()
        response.json.return_value = {
            "success": True,
            "connected": True,
        }
        with mock.patch.object(whatsapp.requests, "get", return_value=response) as get:
            result = whatsapp.get_account_info()

        get.assert_called_once_with(
            f"{whatsapp.WHATSAPP_API_BASE_URL}/health",
            timeout=5,
        )
        self.assertEqual(result["account"], whatsapp.WHATSAPP_ACCOUNT_NAME)
        self.assertTrue(result["connected"])


if __name__ == "__main__":
    unittest.main()
