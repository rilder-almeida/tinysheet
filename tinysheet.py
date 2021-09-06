from __future__ import annotations

from tinydb.database import TinyDB
from tinysheet.tablesheet import TableSheet


class TinySheet(TinyDB):
    table_class = TableSheet

    def sheet(self, name: str, **kwargs) -> TableSheet:

        _recycled = '{}_recycled'.format(name)
        if _recycled not in self._tables:
            self.table_class(self.storage, _recycled, **kwargs)

        if '_config' not in self._tables:
            self.table_class(self.storage, '_config', **kwargs)

        if name in self._tables:
            return self._tables[name]

        table = self.table_class(self.storage, name, **kwargs)
        self._tables[name] = table
        return table

    def table(self, name: str, **kwargs) -> TableSheet:
        return self.sheet(name, **kwargs)
