import pymysql
import json
from datetime import date

def get_data(source_name):
    try:


        db_connection = pymysql.connect(
            host='localhost',
            user='root',
            password='vanshita1234@',
            database='python_training',
        )
        cursor = db_connection.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS schedule_revision_details (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            source_name ENUM('MH', 'WR') NOT NULL,
            data_date DATE NOT NULL,
            revision_no INT NOT NULL,
            created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS schedule_transaction_details (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            sch_rev_details_id BIGINT NOT NULL,
            data_category VARCHAR(255),
            buyer_name VARCHAR(255),
            seller_name VARCHAR(255),
            data_sub_category VARCHAR(255),
            revision_no INT NOT NULL,
            created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (sch_rev_details_id) REFERENCES schedule_revision_details(id)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS schedule_blockwise_data (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            tran_details_id BIGINT NOT NULL,
            block_no INT NOT NULL,
            block_value FLOAT NOT NULL,
            created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (tran_details_id) REFERENCES schedule_transaction_details(id)
        );
        """)

        with open('data_set_python_training.json','r') as file:
            data = json.load(file)

        current_date= date.today()

        check= """
        SELECT revision_no 
        FROM schedule_revision_details 
        WHERE source_name = %s AND data_date = %s
        """
        cursor.execute(check, (source_name, current_date))
        result = cursor.fetchall()

       # print(len(result))

        if len(result) != 0:
            temp=len(result)

            revision_no=result[temp - 1][0] + 1
        else:
            revision_no=1

        insert_revision_query = """INSERT INTO schedule_revision_details (source_name, data_date, revision_no)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_revision_query, (source_name, current_date, revision_no))
        sch_rev_details_id = cursor.lastrowid

      
        for record in data:
            if record.get('source_name') == source_name:
            
                insert_transaction_query = """INSERT INTO schedule_transaction_details 
                    (sch_rev_details_id, data_category, buyer_name, seller_name, data_sub_category, revision_no)
                    VALUES (%s, %s, %s, %s, %s, %s)"""
                values = (
                    sch_rev_details_id,
                    record.get('sch_data_category', 'Default Category'),
                    record.get('sch_buyer_name', 'Default Buyer'),
                    record.get('sch_seller_name', 'Default Seller'),
                    record.get('sch_sub_data_category', 'Default Subcategory'),
                    revision_no,
                )
                cursor.execute(insert_transaction_query, values)
                tran_details_id = cursor.lastrowid
               

                blocks= record.get('block_value',[])
                block_no =1 
                for block_value in blocks:
                    insert_block_query = """INSERT INTO schedule_blockwise_data
                            (tran_details_id, block_no, block_value)
                            VALUES (%s, %s, %s)"""
                block_values =(tran_details_id, block_no, block_value)
    
                cursor.execute(insert_block_query, block_values)
                block_no=block_no+1 


              

        db_connection.commit()
        print(f"Data processed for source_name:{source_name}, revision_no:{revision_no}")

    except Exception as e:
        print(f"Error occurred at line:{e.__traceback__.tb_lineno}")
   
get_data("MH")


