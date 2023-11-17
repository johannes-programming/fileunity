import tomllib as _tomllib

import pandas as _pd
import tomli_w as _tomli_w


class _Stream:
    @classmethod
    def unitclass(cls):
        return cls._unitclass
    def __init__(self, file):
        self.file = file
    def read(self):
        return cls._unitclass.load(self.file)
    def write(self, unit):
        if type(unit) is not self._unitclass:
            raise TypeError
        unit.save(self.file)
    def __str__(self):
        cls = type(self)
        return f"{cls}(file={self.file})"
    def __repr__(self):
        return str(self)

class _Empty:
    pass

class BaseUnit:
    # abstract
    @classmethod
    def data_duplicating(data):
        raise NotImplementedError
    @classmethod
    def data_loading(cls, file):
        raise NotImplementedError
    @classmethod
    def data_saving(cls, file, data):
        raise NotImplementedError
    @classmethod
    def data_default(cls):
        raise NotImplementedError

    #solid
    def __init__(self, data=_Empty):
        cls = type(self)
        if data is _Empty:
            data = self.data_default()
        if type(data) is cls:
            data = data._data
        self.data = data
    @property
    def data(self):
        return self.data_duplicating(self._data)
    @data.setter
    def data(self, value):
        self._data = self.data_duplicating(value)
    @classmethod
    def load(cls, file):
        return cls(cls.data_loading(file))
    def save(self, file):
        self.data_saving(file, self._data)

    @classmethod
    def streamclass(self):
        cls = type(self)
        try:
            return cls.streamclass
        except:
            pass
        cls.streamclass = type(
            f"{cls}Stream",
            [_Stream],
            {'_unitclass':cls},
        )
        return cls.streamclass
    @classmethod
    def stream(cls, file):
        return cls.streamclass()(file)


class StrBasedUnit(BaseUnit):
    # abstract
    @classmethod
    def data_by_str(cls, string):
        raise NotImplementedError
    @classmethod
    def str_by_data(cls, string):
        raise NotImplementedError

    # overwrite
    @classmethod
    def data_duplicating(cls, data):
        string = cls.str_by_data(data)
        return cls.data_by_str(string)
    @classmethod
    def data_loading(cls, file):
        with open(file, "r") as s:
            string = s.read()
        if string.endswith('\n'):
            string = string[:-1]
        return cls.data_by_str(string)
    @classmethod
    def data_saving(cls, file, data):
        string = cls.str_by_data(data)
        if file is None:
            print(string)
            return
        with open(file, "w") as stream:
            stream.write(string + '\n')

    # solid
    @classmethod
    def by_str(cls, string):
        return cls(cls.data_by_str(string))
    def __str__(self):
        return self.str_by_data(self.data)
    def __repr__(self):
        return str(self)

class TextUnit(StrBasedUnit):
    # overwrite
    @classmethod
    def data_by_str(cls, string):
        return str(string).split('\n')
    @classmethod
    def str_by_data(cls, data):
        return '\n'.join(str(x) for x in data)
    @classmethod
    def data_default(cls, file, data):
        return list()

    # solid
    def clear(self):
        self._data.clear()
    def __getitem__(self, key):
        return self._data[key]
    def __setitem__(self, key, value):
        data = self.data
        data[key] = value
        self.data = data
    def __delitem__(self, key):
        data = self.data
        del data[key]
        self.data = data
    def __iter__(self):
        return (x for x in self._data)
    def __len__(self):
        return len(self._data)
    def __str__(self):
        return '\n'.join(self._data)
    def __add__(self, other):
        cls = type(self)
        other = cls(other)
        return cls(self._data + other._data)
    def __radd__(self, other):
        cls = type(self)
        other = cls(other)
        return cls(other._data + self._data)
    def __mul__(self, other):
        cls = type(self)
        data = self._data * other
        return cls(data)
    def __rmul__(self, other):
        cls = type(self)
        data = other * self._data
        return cls(data)
    def __contains__(self, other):
        return (other in self._data)


class TOMLUnit(StrBasedUnit):
    # overwrite
    @classmethod
    def data_default(cls):
        return dict()
    @classmethod
    def str_by_data(cls, data):
        return _tomli_w.dumps(data)
    @classmethod
    def data_by_str(cls, string):
        return _tomllib.loads(string)

    # solid
    @classmethod
    def _getitem(cls, data, key):
        if type(key) is str:
            key = [key]
        for k in key:
            data = data[k]
        return data
    def __getitem__(self, key):
        return self._getitem(self.data, key)
    def __setitem__(self, key, value):
        if type(key) is str:
            key = [key]
        *findkeys, lastkey = key
        data = self.data
        obj = self._getitem(data, findkeys)
        obj[lastkey] = value
        self.data = data
    def __delitem__(self, key):
        if type(key) is str:
            key = [key]
        *findkeys, lastkey = key
        data = self.data
        obj = self._getitem(data, findkeys)
        del obj[lastkey]
        self.data = data
    def __len__(self):
        return len(self._data)
    def __add__(self, other):
        cls = type(self)
        other = cls(other)
        x = dict(**self._data, **other._data)
        return cls(x)
    def __radd__(self, other):
        cls = type(self)
        other = cls(other)
        x = dict(**other._data, **self._data)
        return cls(x)
    def clear(self):
        self._data.clear()
    def keys(self):
        x = self._data.keys()
        x = list(x)
        x = (y for y in x)
        return x
    def values(self):
        x = self._data.values()
        x = list(x)
        x = (y for y in x)
        return x
    def items(self):
        x = self._data.items()
        x = list(x)
        x = (y for y in x)
        return x

 


class Simple_TSVUnit(StrBasedUnit):
    @classmethod
    def data_default(cls):
        return _pd.DataFrame({})
    @classmethod
    def str_by_data(cls, data):
        data = _pd.DataFrame(data)
        lines = list()
        lines.append(list(data.columns))
        for i, row in data.iterrows():
            lines.append(list(row))
        h, w = data.shape
        h += 1
        for y in range(h):
            for x in range(w):
                lines[y][x] = str(lines[y][x])
                if '"' in lines[y][x]:
                    raise ValueError
                if '\t' in lines[y][x]:
                    raise ValueError
            lines[y] = '\t'.join(lines[y])
        return TextUnit.str_by_data(lines)
    @classmethod
    def data_by_str(cls, string):
        lines = TextUnit.data_by_str(string)
        for y in range(len(lines)):
            if '"' in lines[y]:
                raise ValueError
            lines[y] = lines[y].split('\t')
        columns = lines.pop(0)
        if len(set(columns)) != len(columns):
            raise ValueError
        return _pd.DataFrame(lines, columns=columns)



    @property
    def fieldnames(self):
        return list(self._data.columns)
    @fieldnames.setter
    def fieldnames(self, value):
        value = [str(x) for x in value]
        self._data.columns = value
    @property
    def shape(self):
        return tuple(self._data.shape)
    @property
    def width(self):
        return self.shape[1]
    @property
    def height(self):
        return self.shape[0]
    def _parse_key_to_pair(self, key):
        if type(key) is tuple:
            return key
        if key is None:
            return (None, None)
        if type(key) is str:
            return (key, None)
        if type(key) is int:
            return (None, key)
        if type(key) is slice:
            return (None, key)
        if type(key) is list:
            if all((type(x) is str) for x in key):
                return (key, None)
            else:
                return (None, key)
        raise TypeError
    def _parse_key(self, key):
        xkey, ykey = self._parse_key_to_pair(key)
        ykey = self._parse_ykey(ykey)
        return (xkey, ykey)
    def _parse_ykey(self, key):
        if key is None:
            return list(range(self.height))
        if type(key) is int:
            return key
        if type(key) is slice:
            return self._list_by_slice(key)
        if type(key) is list:
            ans = list()
            for k in key:
                ans += self._list_by_listitem(k)
            return ans
        raise TypeError
    def _list_by_listitem(self, item):
        if type(item) is int:
            return [item]
        if type(item) is slice:
            return self._list_by_slice(item)
        raise TypeError
    def _list_by_slice(self, key):
        indeces = list(range(self.height))
        indeces = indeces[key]
        return indeces
    def __delitem__(self, key):
        xkey, ykey = self._parse_key(key)
        if xkey is None:
            xkey = self.fieldnames
        if type(xkey) is str:
            xkey = [xkey]
        if type(ykey) is int:
            ykey = [ykey]
        if (type(xkey) is list) and (type(ykey) is list):
            self._data.drop(columns=xkey, inplace=True)
            self._data.drop(index=ykey, inplace=True)
            return
        raise TypeError
    def __getitem__(self, key):
        cls = type(self)
        xkey, ykey = self._parse_key(key)
        if xkey is None:
            if type(ykey) is int:
                return self._data.loc[ykey].to_dict()
            if type(ykey) is list:
                return cls(self._data.loc[ykey])
        elif type(xkey) is str:
            if type(ykey) is int:
                return str(self._data.at[ykey, xkey])
            if type(ykey) is list:
                return self._data.loc[ykey, xkey].tolist()
        elif type(xkey) is list:
            if type(ykey) is int:
                return self._data.loc[ykey, xkey].tolist()
            if type(ykey) is list:
                return cls(self._data.loc[ykey, xkey])
        raise TypeError
    def __setitem__(self, key, value):
        cls = type(self)
        xkey, ykey = self._parse_key(key)
        if xkey is None:
            if type(ykey) is int:
                self.updaterow(index=ykey, updates=value)
                return
            if type(ykey) is list:
                raise NotImplementedError
        if type(xkey) is str:
            if type(ykey) is int:
                self.setelem(column=xkey, index=ykey, value=value)
                return
            if type(ykey) is list:
                self.setcolumnelems(column=xkey, indeces=ykey, values=value)
                return
        if type(xkey) is list:
            if type(ykey) is int:
                self.setrowelements(columns=xkey, index=ykey, values=value)
                return
            if type(ykey) is list:
                self.setblock(columns=xkey, indeces=ykey, data=value)
                return
        raise TypeError
    def setblock(self, columns, indeces, data):
        cls = type(self)
        data = cls(data).data
        newrows = [row.to_dict() for i, row in data.iterrows()]
        indeces = list(indeces)
        length = max(len(indeces), len(newrows))
        for n in range(length):
            self.updaterow(
                index=indeces[n],
                updates=newrows[n],
            )
    def setcolumnelems(self, column, indeces, values):
        indeces = list(indeces)
        values = list(values)
        length = max(len(indeces), len(values))
        for n in range(length):
            self.setelem(
                column=column, 
                index=indeces[n], 
                value=values[n],
            )
    def setrowelems(self, columns, index, values):
        columns = list(columns)
        values = list(values)
        length = max(len(columns), len(values))
        for n in range(length):
            self.setelem(
                column=columns[n], 
                index=index, 
                value=values[n],
            )
    def updaterow(self, index, updates):
        updates = dict(updates)
        for column, value in updates.items():
            self.setelem(
                column=column, 
                index=index, 
                value=value,
            )
    def setelem(self, column, index, value):
        if type(column) is not str:
            raise TypeError
        if column not in self.fieldnames:
            raise KeyError
        if type(index) is not int:
            raise TypeError
        if (index < 0) or (index >= self.height):
            raise IndexError
        self._data.at[index, column] = str(value)