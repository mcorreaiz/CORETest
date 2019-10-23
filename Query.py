from numpy import random

KLEENE_PROB = 1 / 20
QUERY_TEMPLATE = "SELECT * FROM S WHERE ( {} ) FILTER ( {} );"
FILTER_TEMPLATE = "( {0}[value >= {1}] AND {0}[value <= {2}] )"


class Query:
    def __init__(self, clauses, filters):
        self.clauses = clauses
        self.filters = filters

    def __str__(self):
        events_str = " OR ".join(self.clauses)
        filters_str = " AND ".join((str(f) for f in self.filters))
        return QUERY_TEMPLATE.format(events_str, filters_str)


class Clause:
    def __init__(self, atoms=[], sep=";"):
        self.atoms = atoms
        self.sep = sep

    def _join_atoms(self):
        return " {} ".format(self.sep).join(self.atoms)

    def __str__(self):
        atoms_str = self._join_atoms()
        return "( {} )".format(atoms_str)


class EventClause(Clause):
    def _with_kleene(self):
        kleene = random() < KLEENE_PROB
        return str(self) + ("+" if kleene else "")

    def __str__(self):
        return self._with_kleene()


class Event:
    def __init__(self, event_type, count):
        self.type = event_type
        self.alias = event_type + str(count)

    def __str__(self):
        return "{} AS {}".format(self.type, self.alias)


class QueryFilter:
    def __init__(self, event, low, hi):
        self.event = event
        self.low = low
        self.hi = hi

    def __str__(self):
        return FILTER_TEMPLATE.format(self.event, self.low, self.hi)
