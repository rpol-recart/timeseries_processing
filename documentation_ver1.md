# **Документация проекта: Система предсказаний на основе данных сенсоров**

---

## **1. Общее описание проекта**

Проект представляет собой систему обработки данных с сенсоров, выполняющую **предсказания на основе временных рядов**, с поддержкой **автоматического переключения между сенсорами** и **корректировки параметров** при смене оборудования. Основной сценарий — получение новых измерений, их предобработка, выполнение предсказания и сохранение результата в базу данных.

Система устойчива к сбоям: реализованы **повторные попытки (retry)** при ошибках подключения к БД и обработка **запаздывающих данных (late data)**.

---

## **2. Архитектура и компоненты**

Проект организован по модульному принципу. Основные компоненты:

| Модуль | Назначение |
|-------|-----------|
| `main.py` | Точка входа, координация процесса |
| `config.py` | Конфигурация (БД, таймауты, политики retry) |
| `db/db.py` | Работа с Oracle DB (пул соединений, запросы) |
| `models/` | Модели данных |
| `services/` | Бизнес-логика: получение, обработка, предсказание |
| `utils/` | Вспомогательные утилиты (retry, валидация) |
| `tests/` | Юнит-тесты |

---

## **3. Подробное описание модулей**

---

### **3.1. `config.py` — Конфигурация системы**

```python
# config.py
import os
from datetime import timedelta
import cx_Oracle

# Параметры подключения к Oracle
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pass")
DB_DSN = os.getenv("DB_DSN", "localhost:1521/XE")

# Пул соединений
POOL_MIN = 1
POOL_MAX = 5
POOL_INCREMENT = 1

# Таймауты
PREDICTION_TIMEOUT = 3  # секунды

# Настройки повторных попыток
DB_RETRY_ATTEMPTS = 3
DB_RETRY_DELAY = 1  # начальная задержка (сек)
DB_RETRY_BACKOFF = 2  # множитель (экспоненциальная задержка: 1, 2, 4)
DB_RETRY_EXCEPTIONS = (
    cx_Oracle.DatabaseError,
    cx_Oracle.OperationalError,
    TimeoutError,
)

# Допустимое запаздывание данных
LATE_DATA_TOLERANCE = timedelta(hours=24)
```

#### **Ключевые моменты:**
- Все параметры БД вынесены в переменные окружения.
- Используется **пул соединений** для эффективности.
- Реализована **экспоненциальная задержка** при ошибках БД.
- `LATE_DATA_TOLERANCE` определяет, какие данные считаются "свежими" — до 24 часов после последнего предсказания.

---

### **3.2. `main.py` — Основной процесс**

```python
@retry_db_operation
def main():
    ...
```

#### **Логика выполнения:**
1. Получить последнее предсказание из `table1`.
2. Получить новые измерения из `table2` за последние 24 часа, которые еще не были обработаны.
3. Проверить, изменился ли сенсор:
   - Если **да** — запустить `detect_measurement_shift` для калибровки параметров.
   - Если **нет** — использовать старые `param1`, `param2`.
4. Для каждого измерения:
   - Добавить параметры (`param1`, `param2`).
   - Предобработать данные.
   - Выполнить предсказание.
   - Сохранить результат в `table1`.

#### **Обработка смены сенсора:**
- Находятся измерения на **новом сенсоре** (по одному на устройство).
- Для каждого устройства ищется **последнее измерение на старом сенсоре**.
- Вычисляется **сдвиг сигнала** по совпадающим устройствам.
- Полученные `param1`, `param2` используются для всех последующих предсказаний.

#### **Важно:**
- Используется `@retry_db_operation` для устойчивости.
- Закрытие пула соединений в `finally`.
- Логирование на всех этапах.

---

### **3.3. `db/db.py` — Работа с базой данных**

#### **Класс `DB`**
Управляет пулом соединений к Oracle и предоставляет методы для:

| Метод | Назначение |
|------|-----------|
| `_init_pool()` | Создает пул соединений |
| `get_connection()` | Получает соединение из пула |
| `close_pool()` | Закрывает пул |
| `fetch_last_prediction()` | Последнее предсказание из `table1` |
| `fetch_unprocessed_measurements_last24h()` | Непрошедшие обработку данные за 24 часа |
| `fetch_new_measurements(last_time)` | Данные после указанного времени |
| `fetch_last_measurement_for_sensor_device_before_time()` | Последнее измерение до времени |
| `insert_prediction()` | Сохранение результата предсказания |

#### **Особенности SQL-запросов:**

- **Обнаружение обработанных данных:**
  ```sql
  WHERE NOT EXISTS (
      SELECT 1 
      FROM table1 t1 
      WHERE ABS(EXTRACT(EPOCH FROM (t1.prediction_time - t2.measurement_time))) < 1
  )
  ```
  — Считается, что `prediction_time` и `measurement_time` должны **совпадать по времени** (разница < 1 сек).

- **Использование `LATE_DATA_TOLERANCE`**:
  - Диапазон: `[max_pred_time - 24h, max_pred_time]`.
  - Позволяет обрабатывать запоздавшие данные.

- **Транзакции**: `commit()` при успехе, `rollback()` при ошибке.

---

### **3.4. `models/measurement_data.py` — Модель измерения**

```python
class MeasurementData:
    def __init__(self, sensor_id, device_id, measurement_time, raw_data):
        self.sensor_id = sensor_id
        self.device_id = device_id
        self.measurement_time = measurement_time
        self.raw_data = raw_data  # список временных рядов
        self.param1 = None
        self.param2 = None
```

#### **Структура `raw_data`:**
```json
[
  {
    "ts": [1700000000, 1700000001, ...],
    "feat1": [0.1, 0.2, ...],
    "feat2": [1.0, 1.1, ...]
  },
  ...
]
```

- Каждый элемент — временной ряд с синхронизированными массивами.
- `add_params()` — добавляет калибровочные параметры.

---

### **3.5. `models/preprocessed_measurement.py`**

```python
class PreprocessedMeasurement:
    def __init__(self, processed_data):
        self.data = processed_data
    def to_json(self): ...
```

- Заглушка для сериализации.
- В реальной системе может содержать `numpy.array`, `pandas.DataFrame` и т.п.

---

### **3.6. `services/data_fetcher.py` — Получение данных**

#### **Класс `DataFetcher`**
Интерфейс для получения данных из БД и преобразования в `MeasurementData`.

#### **Методы:**
- `get_last_prediction()` → `dict` или `None`
- `get_new_measurements()` → `[MeasurementData]` (отсортированы по времени)
- `get_last_measurement_for_sensor_device_before(...)` → `MeasurementData` или `None`

#### **Валидация:**
- `json.loads()` для `data` (поддержка CLOB/BLOB).
- Проверка через `is_valid_measurement()`.

---

### **3.7. `services/preprocessing.py` — Предобработка**

```python
def preprocess(measurement: MeasurementData) -> PreprocessedMeasurement:
    processed = {
        "sensor_id": measurement.sensor_id,
        "device_id": measurement.device_id,
        "param1": measurement.param1,
        "param2": measurement.param2,
        "length": sum(len(ts["ts"]) for ts in measurement.raw_data)
    }
    return PreprocessedMeasurement(processed)
```

- Пример: вычисление общей длины временных рядов.
- В реальной системе — нормализация, агрегация, feature engineering.

---

### **3.8. `services/prediction.py` — Предсказание**

```python
def predict(preprocessed: PreprocessedMeasurement) -> float:
    time.sleep(0.1)
    return round(random.uniform(0.1, 0.9), 4)
```

- Заглушка ML-модели.
- Возвращает число от 0.1 до 0.9.
- Может быть заменено на `sklearn`, `pytorch`, `onnx` и т.п.

---

### **3.9. `services/sensor_shift_detector.py` — Детектор сдвига сенсора**

```python
def detect_measurement_shift(
    old_measurements: list[MeasurementData], 
    new_measurements: list[MeasurementData]
) -> tuple[float, float]:
    ...
```

#### **Алгоритм:**
1. Для каждой пары (старое, новое) измерение:
   - Вычислить общую длину временных рядов.
   - Найти относительный сдвиг: `|new_len - old_len| / old_len`.
2. Усреднить сдвиг.
3. Вернуть `param1 = avg_shift * 1.5`, `param2 = avg_shift * 2.0`.

#### **Требования:**
- Списки должны быть одинаковой длины.
- Устройства должны соответствовать (по `device_id`).
- Минимум 2 устройства с данными на обоих сенсорах.

---

### **3.10. `utils/retry.py` — Механизм повторных попыток**

```python
def retry_db_operation(func):
    ...
```

#### **Поведение:**
- Повторяет вызов до `DB_RETRY_ATTEMPTS` раз.
- Задержки: `1, 2, 4` сек (экспоненциальная).
- Перехватывает только `DB_RETRY_EXCEPTIONS`.
- Все остальные исключения пробрасываются.

#### **Пример лога:**
```
WARNING: DB operation failed (attempt 1/3): ORA-12541: TNS:no listener
```

---

### **3.11. `utils/validators.py` — Валидация данных**

```python
def is_valid_measurement(measurement_data) -> bool:
    if not (3 <= len(data) <= 10):  # 3–10 временных рядов
        return False
    for ts in data:
        if not (3000 <= len(ts["ts"]) <= 8000):  # 3k–8k точек
            return False
        if длины feat1, feat2 != длине ts:
            return False
    return True
```

- Проверка структуры и размеров данных.
- Отсекает битые или слишком короткие/длинные измерения.

---

### **3.12. `tests/test_main_flow.py` — Тесты**

```python
def test_main_no_last_prediction(monkeypatch):
    mock_db = MagicMock()
    mock_db.fetch_last_prediction.return_value = None
    monkeypatch.setattr("main.DB", lambda: mock_db)
    with pytest.raises(SystemExit):
        main()
```

- Проверка сценария: нет предыдущего предсказания.
- Использует `unittest.mock` и `pytest`.
- В реальной системе нужно добавить:
  - Тесты на смену сенсора.
  - Тесты на обработку данных.
  - Интеграционные тесты с БД.

---

## **4. Структура базы данных**

### **Таблица `table1` (результаты предсказаний)**
| Поле | Тип | Описание |
|------|-----|---------|
| `prediction_time` | TIMESTAMP | Время предсказания |
| `sensor_id` | INT | ID сенсора |
| `device_id` | INT | ID устройства |
| `param1`, `param2` | FLOAT | Параметры калибровки |
| `result` | FLOAT | Результат предсказания |

### **Таблица `table2` (сырые измерения)**
| Поле | Тип | Описание |
|------|-----|---------|
| `sensor_id` | INT | ID сенсора |
| `device_id` | INT | ID устройства |
| `measurement_time` | TIMESTAMP | Время измерения |
| `data` | CLOB/BLOB | JSON с временными рядами |

---

## **5. Поток данных (Data Flow)**

```
1. Получить последнее предсказание из table1
   ↓
2. Получить новые измерения из table2 (за последние 24 часа, необработанные)
   ↓
3. Если сенсор изменился:
   → Собрать данные по устройствам на старом и новом сенсоре
   → Найти общие устройства (≥2)
   → Вычислить param1, param2 через detect_measurement_shift()
   ↓
4. Для каждого измерения:
   → Добавить param1, param2
   → preprocess()
   → predict()
   → insert_prediction() в table1
```

---

## **6. Правила и ограничения**

- **Минимум 2 устройства** на сенсоре.
- **Минимум 2 общих устройства** при смене сенсора.
- **Данные валидны в течение 24 часов** после последнего предсказания.
- **Время предсказания = времени измерения** (с точностью до 1 сек).
- **Параметры `param1`, `param2` сохраняются между запусками**.

---

## **7. Возможные улучшения и доработки**

| Направление | Рекомендации |
|-----------|-------------|
| **Масштабируемость** | Запуск нескольких экземпляров с блокировками (advisory locks) |
| **Отказоустойчивость** | Добавить health-check, мониторинг |
| **ML-модель** | Реальная модель (sklearn, ONNX), кэширование, версионирование |
| **Конфигурация** | YAML/JSON конфиг вместо `config.py` |
| **Тесты** | Больше unit и интеграционных тестов |
| **Логирование** | Structured logging (JSON), уровень DEBUG для деталей |
| **Обработка ошибок** | Отдельная таблица для ошибок, retry с backoff |
| **CI/CD** | Docker, GitHub Actions, тесты, деплой |

---

## **8. Заключение**

Проект хорошо структурирован, с четким разделением ответственности. Реализована устойчивость к ошибкам БД и гибкая обработка смены сенсоров. Для дальнейшего развития рекомендуется:
- Добавить полноценные тесты.
- Интегрировать реальную ML-модель.
- Настроить мониторинг и логирование.
- Автоматизировать деплой.

---

**Дата документации:** 2025-04-05  
**Версия проекта:** 1.0 (prototype)