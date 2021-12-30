create_schema = ('''
    create schema if not exists petl3;
''')

create_table1=("""
    create table if not exists petl3.viable_countys(
        geo_id int,
        _state_ text,
        county text,
        sales_vector int
        );
""")

insert_table1 = ("""
insert into petl3.viable_countys(geo_id,_state_,county,sales_vector)
    values(%s,%s,%s,%s);

""")


select_table = ('''
    select * from petl3.viable_countys; 
''')