# -*- coding: utf-8 -*-
from configparser import ConfigParser


def parse_ini(file_path):
    parser = ConfigParser(comment_prefixes=('//', '#'))
    parser.read(file_path, encoding='utf-8')
    file_dict = parser._sections
    for section_name in file_dict.keys():
        section = file_dict[section_name]
        for key in section:
            values = parser.get(section_name, key)
            if values is None or values is "":
                file_dict[section_name][key] = None
                continue
            values = values.split(',')
            parsed_values = []
            for value in values:
                if value.isdigit():
                    parsed_values.append(int(value))
                else:
                    try:
                        parsed_values.append(float(value))
                    except:  # value is string
                        parsed_values.append(value)
            file_dict[section_name][key] = parsed_values
    return file_dict
