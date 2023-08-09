import numpy as np

tmp_qry_statement_get_metadata = "SELECT %(cols)s FROM \"%(table_name)s\" WHERE job_id in (%(jobs)s)"

qry_statement_get_metadata = tmp_qry_statement_get_metadata % {
                "table_name": "meta_data_table_v4",
                "jobs": "joba,jobb,jobc,jobd",
                "cols": "col1,col2,col3"
            }
print(qry_statement_get_metadata)


x = np.arange(6)
condlist = [x<3, x>3]
choicelist = [x, x**2]
print(choicelist)
a = np.select(condlist, choicelist, 42)
print(a)