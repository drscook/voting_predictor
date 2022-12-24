from . import *

@dataclasses.dataclass
class Redistricter():
    census_api_key: str
    bg_project_id: str
    state: str = 'TX'
    
    def __post_init__(self):
        self.census = census.Census(self.api_key)
        self.bq = ut.BigQuery(project_id=self.bq_project_id)
        self.state = us.STATES.lookup(self.state)
