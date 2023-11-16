import tomllib as _tomllib

import tomli_w as _tomli_w


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
    def __init__(self, data):
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
    def default(cls):
        return cls(cls.data_default())

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
        raise cls(cls.data_by_str(string))
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

 
