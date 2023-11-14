import Bio.SeqIO as _SeqIO
import seqreads as _sr

import fileunity.basics as _basics


class _SeqReadUnit(_basics.BaseUnit):
    @classmethod
    def data_duplicating(data):
        return _sr.SeqRead(**vars(data))

class SeqReadPHDUnit(_SeqReadUnit):
    @classmethod
    def data_loading(cls, file):
        rec = _SeqIO.read(file, 'phd')
        return _sr.SeqRead.by_seqRecord(rec)
    @classmethod
    def data_saving(cls, file, data):
        rec = data.to_record()
        _SeqIO.write(rec, file, 'phd')


class SeqReadABIUnit(_SeqReadUnit):
    @classmethod
    def data_loading(cls, file):
        rec = _SeqIO.read(file, 'abi')
        return _sr.SeqRead.by_seqRecord(rec)
    @classmethod
    def data_saving(cls, file, data):
        raise NotImplementedError