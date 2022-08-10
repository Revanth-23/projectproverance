# import psycopg2
# def connection():
#     company_type_enum=[]
#     conn = psycopg2.connect(
#         database="resilincscrm", user='postgres', password='postgres', host='172.16.1.236', port= '5432'
#     )
#     cursor= conn.cursor()
#     cursor.execute("SELECT enumlabel FROM PG_ENUM WHERE ENUMTYPID IN ('public.company_type'::REGTYPE) ORDER BY OID")
#     data= cursor.fetchall()
#     for i in data:
#         company_type_enum.append(*i)
#     conn.close()
#     return company_type_enum
import asyncio
import asyncpg
import json 
def connection():
    
    async def run():
        conn = await asyncpg.connect(user='postgres', password='postgres',
                                    database='resilincscrm', host='172.16.1.236')
        query = await conn.fetch(
            "SELECT enumlabel FROM PG_ENUM WHERE ENUMTYPID IN ('public.company_type'::REGTYPE) ORDER BY OID",
            
        )
        
    
        await conn.close()
        return query


    l=[]
    loop = asyncio.get_event_loop()
    res=[dict(i.items()) for i in loop.run_until_complete(run())]
    for i in range(len(res)):
        l.append(res[i]['enumlabel'])
    return l


    

    


