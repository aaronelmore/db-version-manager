class Entry:

    def __init__(self, table_name, head, stamp, version, entry_type):
        self.table_name = table_name
        self.table_head = head
        self.stamp = stamp
        self.version = version
        self.entry_type = entry_type
        self.cow = False # Copy on write flag

    def COW(self):
        self.cow = True
        
class Metadata:

    mtdt_id = 0;

    def __init__(self):
        self.entries = dict()
        self.parents = dict()
        self.name_mapping = dict()
        self.version_mapping = dict()

    # create a new entry that has type "table"
    def create(self, table_name):
        if self.name_mapping.has_key(table_name):
            print "Table already exists!"
            return
        stamp = table_name + "_v1"
        new_entry = Entry(table_name, table_name, stamp, 1, "table")
        self.entries[Metadata.mtdt_id] = new_entry
        self.parents[Metadata.mtdt_id] = [-1] # ancestor being -1 means there's no parent: this is the first version of this table
        self.name_mapping[table_name] = Metadata.mtdt_id
        self.version_mapping[table_name] = 2
        Metadata.mtdt_id += 1
    
    def fork(self, new_table_name, old_table_name):
        old_entry = self.entries[self.name_mapping[old_table_name]]
        version = self.version_mapping[old_entry.table_head]
        new_stamp = old_entry.table_head + "_v" + str(version)
        new_entry = Entry(new_table_name, old_entry.table_head, new_stamp, version, "table")
        self.entries[Metadata.mtdt_id] = new_entry
        self.parents[Metadata.mtdt_id] = [self.name_mapping[old_table_name]]
        self.name_mapping[new_table_name] = Metadata.mtdt_id
        self.entries[self.name_mapping[old_table_name]].COW()
        self.version_mapping[old_entry.table_head] += 1
        Metadata.mtdt_id += 1

    def merge(self, new_table_name, old_table_name_1, old_table_name_2):
        old_entry_1 = self.entries[self.name_mapping[old_table_name_1]]
        old_entry_2 = self.entries[self.name_mapping[old_table_name_2]]
        assert(old_entry_1.table_head == old_entry_2.table_head)

        version = self.version_mapping[old_entry_1.table_head]

        new_stamp = old_entry_1.table_head + "_v" + str(version)
        new_entry = Entry(new_table_name, old_entry_1.table_head, new_stamp, version, "table")
        self.entries[Metadata.mtdt_id] = new_entry
        self.parents[Metadata.mtdt_id] = [self.name_mapping[old_table_name_1], self.name_mapping[old_table_name_2]]
        self.name_mapping[new_table_name] = Metadata.mtdt_id
        self.entries[self.name_mapping[old_table_name_1]].COW()
        self.entries[self.name_mapping[old_table_name_2]].COW()
        self.version_mapping[old_entry_1.table_head] += 1
        Metadata.mtdt_id += 1
        
    def ancestors(self, table_name):
        entry_id = self.name_mapping[table_name]
        if self.parents[entry_id][0] == -1:
            return []

        ancs = set()
        for parent in self.parents[entry_id]:
            ancs.add(parent)

        for anc in ancs:
            more_anc = self.ancestors(self.entries[anc].table_name)
            ancs = set().union(ancs, more_anc)
        
        return ancs
