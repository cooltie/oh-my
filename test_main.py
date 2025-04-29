import pytest
from main import encrypt_telegram_id, decrypt_telegram_id

# Тестовые данные
TEST_TELEGRAM_ID = "123456789"
ENCRYPTION_KEY = "your-encryption-key"  # Замените на ваш ключ шифрования


@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    # Устанавливаем переменную окружения для ключа шифрования
    monkeypatch.setenv("ENCRYPTION_KEY", ENCRYPTION_KEY)


def test_encrypt_decrypt_telegram_id():
    # Шифруем и затем дешифруем идентификатор
    encrypted_id = encrypt_telegram_id(TEST_TELEGRAM_ID)
    decrypted_id = decrypt_telegram_id(encrypted_id)

    # Проверяем, что после дешифрования получаем исходный идентификатор
    assert decrypted_id == TEST_TELEGRAM_ID