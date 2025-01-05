import pandas as pd  # Импортируем библиотеку pandas для работы с данными
import io  # Импортируем io для работы с потоками строк
import os  # Импортируем os для работы с файловой системой
import sys  # Импортируем sys для работы с аргументами командной строки

# Проверяем, передан ли аргумент командной строки (имя файла)
if len(sys.argv) < 2:
    print("Usage: python script.py <path/to/input_file.txt>")
    sys.exit(1)  # Завершаем программу, если имя файла не передано

# Считываем имя входного файла из аргументов командной строки
input_file_name = sys.argv[1]

# Открываем файл для чтения
with open(input_file_name, "r") as input_file:
    section_data = []  # Список для временного хранения данных секции
    current_section = None  # Переменная для хранения текущей секции
    dfs = {}  # Словарь для хранения DataFrame секций

    # Читаем файл построчно
    for line in input_file:
        line = line.strip()  # Удаляем пробелы в начале и конце строки
        # Проверяем, начинается ли строка с "[" (означает начало новой секции)
        if line.startswith("["):
            # Если есть текущая секция, сохраняем её в словарь
            if current_section is not None:
                # Конвертируем данные секции в DataFrame и сохраняем
                section_df = pd.DataFrame(section_data)
                dfs[current_section] = section_df

            current_section = line[1:-1]  # Извлекаем имя секции (удаляем скобки)
            section_data = []  # Сбрасываем временные данные для новой секции
        else:
            # Если строка не является заголовком секции, добавляем её в данные текущей секции
            section_data.append(line.split("\t"))  # Разделяем строку по табуляции

    # Сохраняем последнюю секцию, если она существует
    if current_section is not None:
        section_df = pd.DataFrame(section_data)
        dfs[current_section] = section_df

# Создаем выходную папку, если она не существует
output_dir = os.path.join(os.path.dirname(input_file_name), "output")
os.makedirs(output_dir, exist_ok=True)

# Удаляем ненужные колонки для секции "Probes"
if "Probes" in dfs:
    dfs["Probes"] = dfs["Probes"].drop(
        columns=["unnecessary_column1", "unnecessary_column2"], errors="ignore"
    )

# Сохраняем каждую секцию в отдельный файл формата TSV
for section_name, df in dfs.items():
    output_file_name = os.path.join(output_dir, f"{section_name}.tsv")
    df.to_csv(output_file_name, sep="\t", index=False)  # Сохраняем DataFrame в TSV файл
