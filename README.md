# 12_concurrency
Домашнее задание к лекции Concurrency.
Реализация однопоточной версии memc_load.py на потоках при помощи встроенного модуля threading

## Зависимости
- python2
- virtualenv
- memcache

## Install
Получение репозитория

```cmd
git clone https://github.com/shpawel/12_concurrency.git
cd 12_concurrency
```

Установки вирульного окружения Python
```commandline
virtualenv env --python=python2
source env/bin/activate
```
остальные действия производятся в активированном виртуальном окружении python

Установка зависимостей
```commandline
pip install -r requirements.txt
```

## Запуск
Основым исполняемым модулем является **memc_load_thread.py**
```commandline
python memc_load_thread.py
```


### Параметры запуска
|Параметр|Описание|Значение по умолчанию|
|--------|--------|:-------------------|
|-h|Вывод посказки о параметрах запуска скрипта|
|-t|Запуск тестовых данных|
|-l|Имя файла для лога|
|--dry|Вывод операций без их выполнения|False
|--pattern|Шаблон для поиска файлов|/data/appsinstalled/*.tsv.gz
