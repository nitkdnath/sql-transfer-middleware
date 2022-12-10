from typing import List


def mappingParser(baseMapping: List, overrideMapping: List, mappingKey: str) -> List:
    """Generates a combined mapping based on two sources. \
Items in the mapping lists are selected (items must exist on both lists), \
and fields in an item dictionary are merged (needs to exist in only one of the mappings, \
overrideMapping will take precedence in case of of them having conflicting values).

    Args:
        baseMapping (List): The base mapping list. It consists of mapping dictionaries.
        overrideMapping (List): An override mapping list. It selects items in the base mapping,\
and overrides fields in base mapping dictionaries.
        mappingKey (str): The key which will be used to map the items in the mapping together.

    Returns:
        mappedList: A combination of the provided mappings.
    """
    mappedList = []
    for item in baseMapping:
        for item2 in overrideMapping:
            if item[mappingKey] == item2[mappingKey]:
                item.update(item2)
                mappedList.append(item)
    return mappedList


def config_sanitizer(config):
    sanitized_config = []
    for table in config:
        table["target"] = table.get("target", table["source"])
        table["schema"] = table.get("schema", "dbo")
        sanitized_columns = []
        for column in table["columns"]:
            column["target"] = column.get("target", column["source"])
            column["type_string"] = column.get("type_string", "varchar(255)")
            sanitized_columns.append(column)
        table["columns"] = sanitized_columns
        table["primary_key"] = table.get("primary_key", table["columns"][0]["target"])
        table["where"] = table.get("where", "TRUE")
        sanitized_config.append(table)
    return sanitized_config
