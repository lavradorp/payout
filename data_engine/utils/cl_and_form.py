import unicodedata

class CleaningFormating:
    @staticmethod
    def columns_cleanings(column):
        n = unicodedata.normalize('NFKD', column).encode('ASCII', 'ignore').decode('ASCII')
        return n.lower().replace(' ', '_')
    