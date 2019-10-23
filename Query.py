from numpy import random

KLEENE_PROB = 1 / 20
QUERY_TEMPLATE = "SELECT * FROM S WHERE ( {} ) FILTER ( {} );"
FILTER_TEMPLATE = "( {0}[value >= {1}] AND {0}[value <= {2}] )"


class Query:
    def __init__(self, clauses, filters):
        self.clauses = clauses
        self.filters = filters

    def __str__(self):
        clauses_str = " OR ".join((str(c) for c in self.clauses))
        filters_str = " AND ".join((str(f) for f in self.filters))
        return QUERY_TEMPLATE.format(clauses_str, filters_str)


class Clause:
    def __init__(self, atoms=[], sep=";"):
        self.atoms = atoms
        self.sep = sep

    def _join_atoms(self):
        return " {} ".format(self.sep).join((str(a) for a in self.atoms))

    def __str__(self):
        atoms_str = self._join_atoms()
        return "( {} )".format(atoms_str)


class EventClause(Clause):
    def __init__(self, atoms=[], sep=";"):
        super().__init__(atoms, sep)

    def _has_kleene(self):
        kleene = random.random_sample() < KLEENE_PROB
        return "+" if kleene else ""

    def __str__(self):
        kleene = self._has_kleene()
        return super().__str__() + kleene


class Event:
    def __init__(self, event_type, count):
        self.type = event_type
        self.alias = "{}_{}".format(event_type, str(count))

    def __str__(self):
        return "({} AS {})".format(self.type, self.alias)


class QueryFilter:
    def __init__(self, event, low, hi):
        self.event = event
        self.low = low
        self.hi = hi

    def __str__(self):
        return FILTER_TEMPLATE.format(self.event.alias, self.low, self.hi)
