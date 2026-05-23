import re

# Укажи имя своего файла с логами
input_file = 'pa_pattenr.txt'
output_file = 'unique_api_map.txt'

unique_patterns = set()

# Регулярное выражение для поиска строк с паттернами
pattern_regex = re.compile(r'\[NEW PATTERN\]:\s+(https?://\S+)')

try:
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            match = pattern_regex.search(line)
            if match:
                # Очищаем URL от возможных лишних пробелов в конце
                url_pattern = match.group(1).strip()
                unique_patterns.add(url_pattern)

    # Сортируем для удобства и записываем в новый файл
    with open(output_file, 'w', encoding='utf-8') as out:
        for pattern in sorted(unique_patterns):
            out.write(pattern + '\n')

    print(f"Готово! Найдено уникальных эндпоинтов: {len(unique_patterns)}")
    print(f"Результат сохранен в: {output_file}")

except FileNotFoundError:
    print(f"Ошибка: Файл {input_file} не найден.")