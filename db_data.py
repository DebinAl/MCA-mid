import time
import string
import random
import json
import pandas
import mysql.connector as database
from pymemcache.client import base

# memchached
mc = base.Client(('localhost','11211'))

# Connect to the MySQL database
try:
    db = database.connect(
        host="34.30.23.118",
        user="andika",
        password="andikawira01",
        database="model_database_debin"
)
except Exception as e:
    print(f"There's an error when connecting to Database: {e}")

cursor = db.cursor()

def db_init():
    cursor.execute(
"""
create table if not exists tb_mahasiswa
(
	id_mahasiswa int auto_increment primary key,
	nim_mahasiswa varchar(12),
	nama_mahasiswa varchar(20)
);
""")
    cursor.execute(
        """
create table if not exists tb_matakuliah
(
	id_matakuliah int auto_increment primary key,
	nama_matakuliah varchar(20),
	deskripsi text
);
        """
    )
    cursor.execute(
        """
create table if not exists tb_krs
(
	id_krs int auto_increment primary key,
	id_mahasiswa int,
	id_matakuliah int,
	foreign key (id_mahasiswa) references tb_mahasiswa(id_mahasiswa),
	foreign key (id_matakuliah) references tb_matakuliah(id_matakuliah)
);
        """
    )

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

def read_mahasiswa(limit: int = None) -> list:
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
    print(f"Read From DB: {end - start:.6f} seconds")
    
    df = pandas.DataFrame(columns=["id_mahasiswa", "nim_mahasiswa", "nama_mahasiswa"], data=result)
    try:
        df.to_csv("./out/data/mahasiswa.csv")
    except Exception as e:
        print(f"failed at exporting file: {e}")
    
    return result

def read_mahasiswa_nim(nim: str):
    sql = "SELECT * FROM tb_mahasiswa where `nim_mahasiswa` = %s LIMIT 1" % str(nim)
    cursor.execute(sql)
    result = cursor.fetchall()
    return result

    # TODO
def read_mahasiswa_nim_v2(nim: str):
    if not mc.get("read_%s" % str(nim)):
        sql = "SELECT * FROM tb_mahasiswa where `nim_mahasiswa` = %s LIMIT 1" % str(nim)
        cursor.execute(sql)
        result = cursor.fetchall()
        mc.set("read_%s" % str(nim), json.dump(result))
        return result

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
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    sql = "DELETE FROM tb_mahasiswa WHERE id_mahasiswa = %s"
    values = (id,)
    cursor.execute(sql, values)
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
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
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute("TRUNCATE TABLE tb_krs")
    cursor.execute("TRUNCATE TABLE tb_mahasiswa")
    cursor.execute("TRUNCATE TABLE tb_matakuliah")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    
    cursor.execute("ALTER TABLE tb_mahasiswa AUTO_INCREMENT = 1")
    cursor.execute("ALTER TABLE tb_matakuliah AUTO_INCREMENT = 1")
    cursor.execute("ALTER TABLE tb_krs AUTO_INCREMENT = 1")

    print("Resetted!")
    db.commit()

def generate_nim():
    return str(random.randint(100, 999)) + str(random.randint(100, 999)) + str(random.randint(100, 999))

def convert_nim(nim):
    alphabets = [char for char in string.ascii_letters if not char.isdigit()]

    result = ''.join(random.choice(alphabets) for _ in nim)

    return result

def generate_sentence(num_words: int) -> str:
    words = [''.join(random.choices(string.ascii_letters, k=5)) for _ in range(num_words)]
    return ' '.join(words).capitalize() + '.'

def scenario():
    amount = 20
    amount_matkul = 5
    krs_per_person = 5

    # insert mahasiswa
    start = time.time()
    for i in range(amount):
        nim = generate_nim()
        insert_mahasiswa(nim, convert_nim(nim))
    end = time.time()
    print(f"Insert mahasiswa ({amount}): {end - start:.6f} seconds")

    # insert matakuliah
    start = time.time()
    for i in range(amount_matkul):
        insert_matakuliah(generate_sentence(2), generate_sentence(5))
    end = time.time()
    print(f"Insert matakuliah ({amount_matkul}): {end - start:.6f} seconds")

    # insert krs
    start = time.time()
    for i in range(amount):
        for j in range(krs_per_person):
            id_matkul = random.randint(1, amount_matkul)
            insert_krs(i+1, id_matkul)
    end = time.time()
    print(f"Insert krs ({amount*krs_per_person}): {end - start:.6f} seconds")

    result = read_mahasiswa()

    # read nim v1
    start = time.time()
    for data in result:
        read_mahasiswa_nim(data[1])
    end = time.time()
    print(f"Read mahasiswa ({len(result)}) v1: {end - start:.6f} seconds")

    # read nim v2
    # start = time.time()
    # for data in result:
    #     read_mahasiswa_nim_v2(data[1])
    # end = time.time()
    # print(f"Read {len(result)} row (v2): {end - start:.6f} seconds")

    # delete
    # for i in range(2000):
    #     delete_mahasiswa(i+1)
    # for i in range(10):
    #     delete_krs(i+1)
    
    start = time.time()
    for id in result:
        delete_mahasiswa(id[0])
    end = time.time()
    print(f"delete mahasiswa ({len(result)}): {end - start:.6f} seconds")
    
    reset_db()

def main() -> None:
    scenario()
    # reset_db()

if __name__ == "__main__":
    main()