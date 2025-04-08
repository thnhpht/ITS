import mysql.connector
from mysql.connector import Error

# Hàm check request_id có tồn tại không
def check_request_id(request_id):
    try:                        
        # Thiết lập kết nối đến database
        conn = mysql.connector.connect(
            host= 'localhost',       
            database= 'test',       
            user= 'root',              
            password= '123456'  
        )

        if conn.is_connected():
            cursor = conn.cursor()

            # Check request_id có tồn tại không
            query = "SELECT * FROM ticket WHERE request_id = %s"
            cursor.execute(query, (request_id, ))
            result = cursor.fetchone()

            # Nếu request_id tồn tại update, ngược lại create
            if result:
                print("update")
            else:
                print("create")

    except Error as e:
        print(f"Error occurred: {e}")
        
    finally:
        #  Đóng kết nối
        if conn.is_connected():
            cursor.close()
            conn.close()

# Call the function to execute operations
check_request_id(None)
