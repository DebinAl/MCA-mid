import time
import string
import random
import pandas
import mysql.connector as database
# Connect to the MySQL database

try:
    db = database.connect(
        host="localhost",
        user="root",
        password="",
        database="mca_tubes"
)
except Exception as e:
    print(f"There's an error when connecting to Database: {e}")

cursor = db.cursor()

# CRUD
# CREATE

def insert_mahasiswa(nim: str, nama: str) -> None:
    sql = "INSERT INTO tb_mahasiswa (nim_mahasiswa, nama_mahasiswa) VALUES (%s, %s)"
    values = (nim, nama)
    cursor.execute(sql, values)
    db.commit()

def insert_matakuliah(nama: str, deskripsi: str) -> None:
    sql = "INSERT INTO tb_matakuliah (nama_matakuliah, deskripsi) VALUES (%s, %s)"
    values = (nama, deskripsi)
    cursor.execute(sql, values)
    db.commit()

def insert_krs(id_mahasiswa: str, id_matakuliah: str) -> None:
    sql = "INSERT INTO tb_krs (id_mahasiswa, id_matakuliah) VALUES (%s, %s)"
    values = (id_mahasiswa, id_matakuliah)
    cursor.execute(sql, values)
    db.commit()

# READ

def read_mahasiswa(limit: int = None) -> None:
    if limit == None:
        sql = "SELECT * FROM tb_mahasiswa"
        start = time.time()
        cursor.execute(sql)
    else:
        sql = "SELECT * FROM tb_mahasiswa LIMIT %s"
        values = limit
        start = time.time()
        cursor.execute(sql, values)
        
    result = cursor.fetchall()
    end = time.time()
    print(f"Time taken: {end - start:.6f} seconds")
    
    df = pandas.DataFrame(columns=["id_mahasiswa", "nim_mahasiswa", "nama_mahasiswa"], data=result)
    try:
        df.to_csv("./out/data/mahasiswa.csv")
    except Exception as e:
        print(f"failed at exporting file: {e}")

def read_matakuliah(limit: int = None) -> None:
    if limit == None:
        sql = "SELECT * FROM tb_matakuliah"
        cursor.execute(sql)
    else:
        sql = "SELECT * FROM tb_matakuliah LIMIT %s"
        values = limit
        cursor.execute(sql, values)
        
    result = cursor.fetchall()

    df = pandas.DataFrame(columns=["id_matakuliah", "nama_mahasiswa", "deskripsi"], data=result)
    try:
        df.to_csv("./out/data/matakuliah.csv")
    except Exception as e:
        print(f"failed at exporting file: {e}")

def read_krs(limit: int = None) -> None:
    if limit == None:
        sql = "SELECT * FROM tb_krs"
        cursor.execute(sql)
    else:
        sql = "SELECT * FROM tb_krs LIMIT %s"
        values = limit
        cursor.execute(sql, values)
        
    result = cursor.fetchall()

    df = pandas.DataFrame(columns=["id_krs", "id_mahasiswa", "id_matakuliah"], data=result)
    try:
        df.to_csv("./out/data/krs.csv")
    except Exception as e:
        print(f"failed at exporting file: {e}")

# UPDATE

def update_mahasiswa(nim: str, nama: str, id: str) -> None:
    sql = "UPDATE tb_mahasiswa SET nim_mahasiswa = %s, nama_mahasiswa = %s WHERE id_mahasiswa = %s"
    values = (nim, nama, id)
    cursor.execute(sql, values)
    db.commit()

def update_matakuliah(nama: str, deskripsi: str, id: str) -> None:
    sql = "UPDATE tb_matakuliah SET nama_matakuliah = %s, deskripsi = %s WHERE id_matakuliah = %s"
    values = (nama, deskripsi, id)
    cursor.execute(sql, values)
    db.commit()

def update_krs(id_mahasiswa: str, id_matakuliah: str, id_krs: str) -> None:
    sql = "UPDATE tb_krs SET id_mahasiswa = %s, id_matakuliah = %s WHERE id_krs = %s"
    values = (id_mahasiswa, id_matakuliah, id_krs)
    cursor.execute(sql, values)
    db.commit()

# DELETE
def delete_mahasiswa(id: str) -> None:
    sql = "DELETE FROM tb_mahasiswa WHERE id_mahasiswa = %s"
    values = (id,)
    cursor.execute(sql, values)
    db.commit()

def delete_matakuliah(id: str) -> None:
    sql = "DELETE FROM tb_matakuliah WHERE id_matakuliah = %s"
    values = (id,)
    cursor.execute(sql, values)
    db.commit()

def delete_krs(id: str) -> None:
    sql = "DELETE FROM tb_krs WHERE id_krs = %s"
    values = (id,)
    cursor.execute(sql, values)
    db.commit()

def reset_db() -> None:
    sql1 = "ALTER TABLE tb_mahasiswa AUTO_INCREMENT = 1"
    sql2 = "ALTER TABLE tb_matakuliah AUTO_INCREMENT = 1"
    sql3 = "ALTER TABLE tb_krs AUTO_INCREMENT = 1"
    
    cursor.execute(sql1)
    cursor.execute(sql2)
    cursor.execute(sql3)
    db.commit()

def generate_nim() -> None:
    return str(random.randint(100, 999)) + str(random.randint(100, 999)) + str(random.randint(100, 999))

def convert_nim(nim):
    alphabets = [char for char in string.ascii_letters if not char.isdigit()]

    result = ''.join(random.choice(alphabets) for _ in nim)

    return result

def generate_sentence(num_words: int) -> str:
    words = [''.join(random.choices(string.ascii_letters, k=random.randint(3, 10))) for _ in range(num_words)]
    return ' '.join(words).capitalize() + '.'



def main() -> None:
    # # delete 1000
    # for i in range(2000):
    #     delete_mahasiswa(i+1)
    # for i in range(10):
    #     delete_krs(i+1)

    #insert mahasiswa
    # for i in range(1000):
    #     nim = generate_nim()
    #     insert_mahasiswa(nim, convert_nim(nim))

    # insert matakuliah
    # for i in range(750):
    #     insert_matakuliah(generate_sentence(2), generate_sentence(5))
    
    # insert krs
    # for i in range(1000):
    #     for amount in range(5):
    #         id_matkul = random.randint(1, 750)
    #         insert_krs(i+1, id_matkul)

    read_mahasiswa()


    # reset_db()

if __name__ == "__main__":
    main()