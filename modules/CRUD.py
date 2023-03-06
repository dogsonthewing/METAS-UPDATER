
#Insere pedidos no BigQuery -- aka CREATE
def insertOrders(date , store , goal):
    rows_to_insert = [
            {u'data': str(date) , u'ecommerceName': str(store) , u'meta': str(goal)}
            ]
        
    errors = client.insert_rows_json("sacred-drive-353312.metas.metas", rows_to_insert)
    if errors == []:
        counter = 1
    else:
        print(f'Encountered errors while inserting rows: {errors}')
    return

#lê dados dos big query -- aka READ
def read(condition):
    query_job = config.client.query("""
        SELECT *
        FROM {}
        WHERE {}
        """.format(config.table_id , condition))  
    orders = query_job.result()
    return orders

#deleta os dados das condições configurada -- aka DELETE
def delete():
    query_job = config.client.query("""
        DELETE FROM {}
        WHERE {};
        """.format(config.table_id , config.interval)) 
    query_job.result()

    return

def update(update , condition):
    query_job = config.client.query("""
        UPDATE {}
        SET {}
        WHERE {};
        """.format(config.table_id , update , condition)) 
    query_job.result()

    print('Rows has been updated.')

    return