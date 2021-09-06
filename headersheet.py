from __future__ import annotations

from typing import Any, Union

from cerberus import TypeDefinition, Validator
from tinydb.table import Table
from tinysheet.fieldsheet import FieldSheet

__all__ = ('HeaderSheet')


class HeaderSheet(dict):

    """
    HeaderSheet is a chained dictionary class, which will permit "one-line
    implementation" to group the created fields. Allowing add, remove and edit
    multiple fileds quickly, setting then any wanted rules.

    For more information about Cerberus Validaton Rules:
    https://docs.python-cerberus.org/en/stable/validation-rules.html

    """

    def __init__(self, name: str):
        """
        :param name: The string used as field name. Posicional only.
        """
        self._name = name
        self['_schema'] = {}

    @property
    def schema(self) -> dict:
        return self['_schema']

    @schema.deleter
    def schema(self):
        self['_schema'] = {}

    def add(self, *fields: Union[str, FieldSheet]) -> 'HeaderSheet':
        """
        >>> name = FieldSheet('name')
        >>> name.required(True).empty(False).type(str)
        {
            "name": {
                "required": True,
                "empty": True,
                "type": "str"
            }
        }
        >>> profile = HeaderSheet()
        >>> profile.add(name)
        {
            "name": {
                "required": True,
                "empty": True,
                "type": "str"
            }
        }
        >>> profile.add('age').rule('type', int, 'age')
        {
            "age": {
                "type": "int"
            },
            "name": {
                "empty": False,
                "required": True,
                "type": "str"
            }
        }
        """
        for field in list(fields):
            if isinstance(field, str):
                field = FieldSheet(field)
            if isinstance(field, FieldSheet):
                self['_schema'] = {**self['_schema'], **field.schema}
        return self

    def registry(self, table: Table, name: str = None):
        name = name if name is not None else self._name
        table.registry.add(name, self['_schema'])

    def remove(self, *fields: Union[str, FieldSheet]) -> 'HeaderSheet':
        """
        >>> profile.remove('age')
        {
            "name": {
                "required": True,
                "empty": False,
                "type": "str"
                "nullable": False
            }
        }
        """
        for field in list(fields):
            if isinstance(field, str):
                field = FieldSheet(field)
            if isinstance(field, FieldSheet):
                self['_schema'].pop(field.name)
        return self

    def rule(self, rule: str, value: Any, /,
             fields: Union[str, list[str]] = None) -> 'HeaderSheet':
        """
        >>> profile.add(FieldSheet('phone').type(int), 'gender').rule(
                'required', False, ['phone', 'gender'])
        {
            "gender": {
                "required": False
            },
            "name": {
                "empty": False,
                "required": True,
                "type": "str"
            },
            "phone": {
                "required": False,
                "type": "int"
            }
        }
        >>> profile.rule('nullable', False)
        {
            "gender": {
                "nullable": False,
                "required": False
            },
            "name": {
                "empty": False,
                "nullable": False,
                "required": True,
                "type": "str"
            },
            "phone": {
                "nullable": False,
                "required": False,
                "type": "int"
            }
        }
        >>> profile.rule('allowed', ['male', 'female'], 'gender')
        {
            "gender": {
                "allowed": [
                    "male",
                    "female"
                ],

                "nullable": False,
                "required": False
            },
            "name": {
                "empty": False,
                "nullable": False,
                "required": True,
                "type": "str"
            },
            "phone": {
                "nullable": False,
                "required": False,
                "type": "int"
            }
        }
        """

        if rule == 'type':
            try:
                if value.__name__ not in list(Validator.types_mapping.keys()):
                    Validator.types_mapping[value.__name__] = TypeDefinition(
                        value.__name__, (value,), ())

                value = value.__name__
            except Exception:
                raise TypeError('Could not assign {} as a type'.format(value))

        if fields is None:
            fields = list(self['_schema'].keys())

        if isinstance(fields, str):
            Validator({fields: {rule: value}}).schema.validate()
            self['_schema'][fields][rule] = value
        elif isinstance(fields, list):
            for k in fields:
                if str(k) in list(self['_schema'].keys()):
                    Validator({str(k): {rule: value}}).schema.validate()
                    self['_schema'][str(k)][rule] = value
        return self

    def all_fields(self) -> list[str]:
        """
        >>> profile.all_fields()
        ['name', 'phone', 'gender']
        """
        return list(self['_schema'].keys())

    def seek(self, rule: str, value: Any = '___missing_value') -> list[str]:
        """
        >>> profile.seek('required', False)
        ['phone', 'gender']
        >>> profile.seek('type')
        ['name', 'phone']
        """
        _fields = []
        for field in list(self['_schema'].keys()):
            if rule in self['_schema'][field]:
                if self['_schema'][field][rule] == value or (
                        value == '___missing_value'):
                    _fields.append(field)
        return _fields

    def __repr__(self):
        return '{}'.format(self['_schema'])
