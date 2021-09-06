from __future__ import annotations

from cerberus import TypeDefinition, Validator
from tinydb.table import Table

__all__ = ('FieldSheet')


class FieldSheet(dict):

    """
    FieldSheet will provide a easy implementation for fields. Like a schema
    generator for data entry with multiple validation rules. Acting as a
    interface class for Ceberus Schema.

    For more information about Cerberus Validaton Rules:
    https://docs.python-cerberus.org/en/stable/validation-rules.html

    >>> name = FieldSheet('name')
    >>> name.required(True).empty(False).type(str)
    {
        "name": {
            "required": True,
            "empty": True,
            "type": "str"
        }
    }
    """

    def __init__(self, name: str):
        """
        :param name: The string used as field name. Posicional only.
        """
        self._name = name
        self['_schema'] = {self._name: {}}

    @property
    def name(self) -> str:
        return self._name

    @property
    def schema(self) -> dict:
        return self['_schema']

    def registry(self, table: Table, name: str = None):
        name = name if name is not None else self._name
        table.registry.add(name, self['_schema'])

    def type(self, type_: type) -> 'FieldSheet':
        # Data type allowed for the key value.
        if hasattr(type_, '__name__'):
            type_name = type_.__name__
        elif hasattr(type_.__class__, '__name__'):
            type_name = type_.__class__.__name__
        elif hasattr(type_, '_name'):
            type_name = type_.__class__.__name__
        else:
            raise TypeError('Could not assign {} as a type'.format(type_))

        try:
            # Applying custom type validation with Cerberus TypeDefinition.
            if type_name not in list(Validator.types_mapping.keys()):
                Validator.types_mapping[type_name] = TypeDefinition(
                    type_name, (type_,), ())
        except Exception:
            raise TypeError('Could not assign {} as a type'.format(type_))
        else:
            return self

    def __getattr__(self, method):
        """
        For each method chained, it gets the value and assign it to the called
        method as key. Returning instance of itself.
        Inspiration: https://stackoverflow.com/a/61120452/15560677
        """
        def value_reciever(value):
            # There are no concrete methods being setted.
            Validator({self.name: {method: value}}).schema.validate()
            self['_schema'][self.name][method] = value
            return self
        return value_reciever

    def __repr__(self):
        return '{}'.format(self['_schema'])
