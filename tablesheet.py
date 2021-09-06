from __future__ import annotations

from typing import (Any, Callable, Iterable, List, Mapping, Optional, Set,
                    Tuple, TypeVar, Union)

from cerberus import Validator, schema_registry
from tinydb.queries import Query, QueryInstance
from tinydb.storages import Storage
from tinydb.table import Table
from tinysheet.headersheet import HeaderSheet

__all__ = ('TableSheet')

cond = TypeVar('cond', bound=Union[Query, QueryInstance, List[Any]])
interval = TypeVar('interval', bound=List[Union[int, Set[int]]])


###############################################################################

class BareTable(Table):

    def __init__(
        self,
        storage: Storage,
        name: str,
    ):
        super().__init__(storage, name)

    #######################################################################

    def insert(self, document: Mapping) -> int:
        return super().insert(self.validated(document))

    def insert_multiple(self, documents: Iterable[Mapping]) -> List[int]:
        return super().insert_multiple(
            [self.validated(document) for document in documents])

    def update(
        self,
        fields: Union[Mapping, Callable[[Mapping], None]],
        cond: Optional[Query] = None,
        doc_ids: Optional[Iterable[int]] = None,
    ) -> List[int]:
        return super().update(self.validated(fields), cond, doc_ids)

    def update_multiple(
        self,
        updates: Iterable[
            Tuple[Union[Mapping, Callable[[Mapping], None]], Query]
        ],
    ) -> List[int]:
        new_updates = []
        for update in updates:
            update = list(update)
            update[0] = self.validated(update[0])
            update = tuple(update)
            new_updates.append(update)
        return super().update_multiple(new_updates)

    #######################################################################

    def raw(self) -> dict:
        return dict(zip(self.get_ids(self.all()), self.all()))

    def get_ordered(
        self,
        documents: Union[interval, List[dict]],
        by: Union[str, List[str]],
        reverse=False
    ) -> List[int]:
        if isinstance(documents, interval):
            documents = self.get_docs(documents)
        by = by if isinstance(by, list) else list(by)
        return sorted(documents,
                      key=lambda dict: list(
                          dict[key].doc_id for key in by if key in [
                              *self.header.all_fields()]),
                      reverse=reverse)

    def get_docs(self, doc_ids: interval) -> List[dict]:
        _doc_list = []
        for _id in self._interval(doc_ids):
            if self.get(doc_id=_id) is not None:
                _doc_list.append(self.get(doc_id=_id))

        return _doc_list

    def get_ids(self, documents: List[dict]) -> List[int]:
        doc_ids = []
        for document in documents:
            if hasattr(document, 'doc_id'):
                doc_ids.append(document.doc_id)
            else:
                raise Exception('Documents objects has no doc_id attribute.')
        return doc_ids

    def _interval(self, doc_ids: interval) -> list[int]:
        _doc_id_set = set()
        for doc_id in doc_ids:
            if isinstance(doc_id, int):
                _doc_id_set.add(doc_id)
            elif isinstance(doc_id, set):
                if len(doc_id) != 2:
                    raise Exception('Set must contain only 2 integers')
                else:
                    _s = list(doc_id)[0]
                    _e = list(doc_id)[1]
                    while _s <= _e:
                        _doc_id_set.add(_s)
                        _s += 1
            else:
                raise TypeError('doc_ids must be interval type.'
                                'List[Union[int, Set[int]]]')
        return list(_doc_id_set)

###############################################################################


class DocModelFactory:

    def model(self, *, data: dict = None):
        class BaseModelObject(dict):
            def __init__(self, /, data: dict = data, **kwargs):
                if isinstance(data, dict):
                    for key in list(data.keys()):
                        setattr(self, key, data[key])
                for k in list(kwargs.keys()):
                    setattr(self, k, kwargs[k])

            def _validate_args(self, key, value):
                _r = ({k: v for k, v in self._validator_rules.items()
                       if k != 'schema'})
                if key in list(self._validator_rules['schema'].keys()):
                    _s = {key: self._validator_rules['schema'].get(key)}
                    _v = Validator(schema=_s, **_r)
                    _result = _v.validated({key: value})
                else:
                    if isinstance(
                            self._validator_rules['allow_unknown'], dict):
                        _s = {key: self._validator_rules['allow_unknown']}
                        _v = Validator(schema=_s, **_r)
                        _result = _v.validated({key: value})
                    else:
                        _result = ({key: value}
                                   if self._validator_rules['allow_unknown']
                                   else None)

                if _result is None:
                    raise Exception(
                        'Validation Errors: {}'.format(_v.errors))
                return _result

            def __setattr__(self, key, value):
                newvalue = self._validate_args(key, value)[key]
                super().__setattr__(key, newvalue)
                super().__setitem__(key, newvalue)

            def __setitem__(self, key, value):
                newvalue = self._validate_args(key, value)[key]
                super().__setattr__(key, newvalue)
                super().__setitem__(key, newvalue)

            def __repr__(self):
                return '{}({})'.format(self.__name__, self.__dict__)

            def validated(self):
                _v = Validator(**self._validator_rules)
                _result = _v.validated(self.__dict__)
                if _result is None:
                    raise Exception(
                        'Validation Errors: {}'.format(_v.errors))
                return _result
        _obj_methods = {
            '__name__': '{}_model'.format(self._name),
            '_validator_rules': self._set_validator_rules(),
        }

        type_model = type(
            '{}_model'.format(self._name),
            (BaseModelObject,),
            _obj_methods)

        return type_model if data is None else type_model(data)

###############################################################################


class ValidatedTable(BareTable, DocModelFactory):
    def __init__(
        self,
        storage: Storage,
        name: str,
        *,
        schema: dict,
        allow_unknown: Union[bool, dict] = True,
        ignore_none_values: bool = False,
        normalize: bool = True,
        purge_unknown: bool = False,
        purge_readonly: bool = False,
        require_all: bool = False,
        raise_on_validation_errors: bool = True,
    ):
        super().__init__(storage, name)

        self._schema_registry = schema_registry

        self._schema = schema
        self._allow_unknown = allow_unknown
        self._ignore_none_values = ignore_none_values
        self._normalize = normalize
        self._purge_unknown = purge_unknown
        self._purge_readonly = purge_readonly
        self._require_all = require_all

    ######################################################################

    @property
    def validator(self) -> Validator:
        return self._get_validator()

    def _get_validator(self, **kwargs):
        _validator_rules = (self._set_validator_rules(**kwargs)
                            if kwargs
                            else self._set_validator_rules())
        return Validator(**_validator_rules)

    def _set_validator_rules(
        self,
        *,
        schema: dict = None,
        allow_unknown: Union[bool, dict] = None,
        ignore_none_values: bool = None,
        normalize: bool = None,
        purge_unknown: bool = None,
        purge_readonly: bool = None,
        require_all: bool = None,
    ):
        return {
            'schema': self.schema if schema is None else schema,

            'allow_unknown': (self._allow_unknown
                              if allow_unknown is None
                              else allow_unknown),

            'ignore_none_values': (self._ignore_none_values
                                   if ignore_none_values is None
                                   else ignore_none_values),

            'normalize': self._normalize if normalize is None else normalize,

            'purge_unknown': (self._purge_unknown
                              if purge_unknown is None
                              else purge_unknown),

            'purge_readonly': (self._purge_readonly
                               if purge_readonly is None
                               else purge_readonly),

            'require_all': (self._require_all
                            if require_all is None
                            else require_all),

            'schema_registry': self._schema_registry
        }

    #######################################################################

    def validate(self, document: dict) -> bool:
        _validator = self.validator
        if not _validator.validate(document):
            raise Exception('Validation Errors: {}'.format(
                _validator.errors))
        return True

    def validated(self, document: dict) -> dict:
        _validator = self.validator
        if not _validator.validate(document):
            raise Exception('Validation Errors: {}'.format(
                _validator.errors))
        return _validator.validated(document)

    def validate_errors(self, document: dict) -> Any:
        return self.validator.validate(document).errors


###############################################################################


class TableSheet(ValidatedTable):

    def __init__(
        self,
        storage: Storage,
        name: str,
        header: HeaderSheet = None,
        **kwargs,
    ):
        self._name = name
        self._header = header if header is not None else HeaderSheet(
            '{self._name}_header'.format)
        self._schema = self._header['_schema']

        self._kwargs = kwargs

        super().__init__(
            storage,
            name,
            schema=self._schema,
            **self._kwargs
        )

    #######################################################################

    @ property
    def header(self) -> 'HeaderSheet':
        return self._header

    @ header.setter
    def header(self, other: HeaderSheet):
        if not isinstance(other, HeaderSheet):
            raise TypeError(
                '{} is not a HeaderSheet type object.'.format(
                    other.__class__)
            )
        self._header = other
        for field in list(other['_schema'].keys()):
            self._schema_registry.add(field, {field: other['_schema'][field]})

    #######################################################################

    @property
    def schema(self) -> dict:
        return self._header['_schema']

    #######################################################################

    @property
    def registry(self) -> dict:
        return self._schema_registry

    #######################################################################

    @property
    def where(self) -> QueryInstance:
        return Query()

    #######################################################################

    @property
    def allow_unknown(self) -> Union[bool, dict]:
        return self._allow_unknown

    @allow_unknown.setter
    def allow_unknown(self, value):
        if not isinstance(value, (bool, dict)):
            raise TypeError(
                '{} is not a bool or dict type object.'.format(
                    value.__class__)
            )
        self._allow_unknown = value

    #######################################################################
    @property
    def ignore_none_values(self) -> bool:
        return self._ignore_none_values

    @ignore_none_values.setter
    def ignore_none_values(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError(
                '{} is not a bool type object.'.format(
                    value.__class__))
        self._ignore_none_values = value

    #######################################################################

    @ property
    def normalize(self) -> bool:
        return self._normalize

    @ normalize.setter
    def normalize(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError(
                '{} is not a bool type object.'.format(
                    value.__class__))
        self._normalize = value

    #######################################################################

    @ property
    def purge_unknown(self) -> bool:
        return self._purge_unknown

    @ purge_unknown.setter
    def purge_unknown(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError(
                '{} is not a bool type object.'.format(
                    value.__class__))
        self._purge_unknown = value

    #######################################################################

    @ property
    def purge_readonly(self) -> bool:
        return self._purge_readonly

    @ purge_readonly.setter
    def purge_readonly(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError(
                '{} is not a bool type object.'.format(
                    value.__class__))
        self._purge_readonly = value

    #######################################################################

    @ property
    def require_all(self) -> bool:
        return self._require_all

    @ require_all.setter
    def require_all(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError(
                '{} is not a bool type object.'.format(
                    value.__class__))
        self._require_all = value

    #######################################################################

    def __repr__(self):
        return super(Table, self).__repr__()
