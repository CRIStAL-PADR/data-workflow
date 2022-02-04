import pandas as pd
import json
import sys
import jmespath

class DDGNode(object):
    def __init__(self, target_object):
        self.dirtyCounter = 1
        self.cleanCounter = 0
        self.inputs = []
        self.outputs = []
        self.target_object = target_object

    def add_input(self, b):
        self.inputs.append(b)
        b.outputs.append(self)

    def is_dirty(self):
        return self.dirtyCounter != self.cleanCounter

    def clean_dirty(self):
        self.cleanCounter = self.dirtyCounter

    def propagate_dirty_flag(self):
        self.increment_dirty_counter()
        for node in self.outputs:
            node.propagate_dirty_flag()

    def increment_dirty_counter(self):
        self.dirtyCounter += 1

    def update(self):
        if self.is_dirty():
            self.clean_dirty()
            for node in self.inputs:
                node.update()
            self.target_object.do_update()

class Data(object):
    def __init__(self, owner, name="Undefined", value=None):
        self.name = name
        self.value = value
        self.ddg_node = DDGNode(self)
        self.parent = None
        self.owner = owner

    def set_value(self, value):
        self.value = value
        self.ddg_node.propagate_dirty_flag()

    def get_value(self):
        if self.ddg_node.is_dirty():
            self.ddg_node.update()
        return self.value

    def set_parent(self, b):
        self.parent = b
        self.ddg_node.add_input(b.ddg_node)

    def add_input(self, target):
        self.ddg_node(target)

    def add_output(self, target):
        target.ddg_node(self)

    def set_owner(self, owner):
        self.owner = owner

    def do_update(self):
        if self.parent != None:
            self.value = self.parent.get_value();
            print("["+self.owner.get_name()+"."+self.name+"] Update from parent")
            return
        print("["+self.owner.get_name()+"."+self.name+"] Update from local value")

class Component(object):
    def __init__(self, name="Undefined"):
        self.__dict__["name"] = name
        self.__dict__["ddg_node"] = DDGNode(self)
        self.__dict__["inputs"] = {}
        self.__dict__["outputs"] = {}
        self.__dict__["datum"] = {}

    def get_name(self):
        return self.__dict__["name"]

    def do_update(self):
        print("[" + self.__dict__["name"] + "]: component update called from ddg graph")

    def add_input_data(self, input):
        self.__dict__["inputs"][input.name] = input
        self.__dict__["datum"][input.name] = input
        input.set_owner(self)
        self.__dict__["ddg_node"].add_input(input.ddg_node)

    def add_output_data(self, output):
        self.__dict__["outputs"][output.name] = output
        self.__dict__["datum"][output.name] = output
        output.set_owner(self)
        output.ddg_node.add_input(self.__dict__["ddg_node"])

    def __setattr__(self, name, value):
        if name in self.__dict__["datum"]:
            self.__dict__["datum"][name].set_value(value)
        else:
            self.__dict__[name] = value

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        if name in self.__dict__["datum"]:
            return self.__dict__["datum"][name]
        raise Exception("No data field named '" + self.__dict__["name"]+"."+name+"'")

class LoadJSON(Component):
    def __init__(self, **kwargs):
        super(LoadJSON, self).__init__(**kwargs)
        self.add_input_data(Data(self, "filename", "undefined"))
        self.add_output_data(Data(self, "content", {}))

    def do_update(self):
        name = self.filename.get_value()
        if name[0:7] == "file://":
            with open(name[7:],"r") as f:
                self.content.set_value(json.load(f))
            return
        elif name[0:7] == "http://":
            pass #return
        raise Exception("Invalid ressource provide: only file:// or http:// prefix are supported")

class LoadXLSX(Component):
    def __init__(self, **kwargs):
        super(LoadXLSX, self).__init__(**kwargs)
        self.add_input_data(Data(self, "filename", "undefined"))
        self.add_output_data(Data(self, "content", pd.DataFrame()))
        self.add_output_data(Data(self, "columns", []))

    def do_update(self):
        name = self.filename.get_value()
        if name[0:7] == "file://":
            fname = name[7:]
            self.content.set_value(pd.read_excel(fname))
            self.columns.set_value(self.content.get_value().columns)
            return
        elif name[0:7] == "http://":
            pass #return
        raise Exception("Invalid ressource provide: only file:// or http:// prefix are supported")


class UnStructuredQuery(Component):
    def __init__(self, **kwargs):
        super(UnStructuredQuery, self).__init__(**kwargs)
        self.add_input_data(Data(self, "query", ""))
        self.add_input_data(Data(self, "source", {}))
        self.add_output_data(Data(self, "content", {}))

    def do_update(self):
        q = jmespath.search(self.query.get_value(), self.source.get_value())
        print("DO THE QUERY", self.query.get_value(), "=>", q)
        self.content.set_value(q)

class UnStructuredToFrame(Component):
    def __init__(self, **kwargs):
        super(UnStructuredToFrame, self).__init__(**kwargs)
        self.add_input_data(Data(self, "type", "records"))
        self.add_input_data(Data(self, "source", {}))
        self.add_output_data(Data(self, "dataframe", {}))

    def do_update(self):
        if self.type.get_value() == "records":
            frame = pd.DataFrame.from_records(self.source.get_value())
            self.dataframe.set_value(frame)
        else:
            raise Exception("unsupported data type")

class Container(Component):
    def __init__(self, **kwargs):
        super(Container, self).__init__(**kwargs)
        self.add_output_data(Data(self, "content", {}))

    def __setitem__(self, name, value):
        g = self.content.get_value()
        g[name] = value
        self.content.set_value(g)

    def __getitem__(self, name):
        return self.content.get_value()[name]

class DataFrameContainer(Component):
    """Tabular data"""
    def __init__(self, **kwargs):
        super(DataFrameContainer, self).__init__(**kwargs)
        self.add_output_data(Data(self, "content",  pd.DataFrame() ))

    def __setitem__(self, name, value):
        raise Exception("Get in Data Frame")

    def __getitem__(self, name):
        dataframe = self.content.get_value()
        return dataframe[name]

    #def __getattr__(self, name):
    #    print("HELLO WORLD", name)

class DataFrameQuery(DataFrameContainer):
    """Tabular data"""
    def __init__(self, **kwargs):
        super(DataFrameQuery, self).__init__(**kwargs)
        self.add_input_data(Data(self, "query", ""))
        self.add_input_data(Data(self, "source",  pd.DataFrame() ))

    def do_update(self):
        print("[" + self.__dict__["name"] + "]: component update called from ddg graph")
        query = self.query.get_value()
        content = pd.eval(query, global_dict={"source" : self.source.get_value()})
        self.content.set_value( content )

class DataFrameRename(DataFrameContainer):
    """Tabular data"""
    def __init__(self, **kwargs):
        super(DataFrameRename, self).__init__(**kwargs)
        self.add_input_data(Data(self, "columns", {}))
        self.add_input_data(Data(self, "source",  pd.DataFrame() ))

    def do_update(self):
        print("[" + self.__dict__["name"] + "]: component update called from ddg graph")
        frame = self.source.get_value()
        self.content.set_value( frame.rename(columns=self.columns.get_value()) )

class DataFrameReplace(DataFrameContainer):
    """Tabular data"""
    def __init__(self, **kwargs):
        super(DataFrameReplace, self).__init__(**kwargs)
        self.add_input_data(Data(self, "by", {}))
        self.add_input_data(Data(self, "source",  pd.DataFrame() ))

    def do_update(self):
        print("[" + self.__dict__["name"] + "]: component update called from ddg graph")
        frame = self.source.get_value()
        self.content.set_value( frame.replace(self.by.get_value()) )

class DataFrameToJSON(Container):
   def __init__(self, **kwargs):
        super(DataFrameToJSON, self).__init__(**kwargs)
        self.add_input_data(Data(self, "source",  pd.DataFrame() ))

   def do_update(self):
        print("[" + self.__dict__["name"] + "]: component update called from ddg graph ..")
        self.content.set_value(self.source.get_value().to_json(orient="table"))
