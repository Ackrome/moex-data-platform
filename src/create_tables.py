# File: src/create_tables.py
import psycopg2
from src.config import settings
from passlib.context import CryptContext

# Настройка хеширования
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password):
    return pwd_context.hash(password)

def run_migration():
    print(f"🔌 Connecting to DB at {settings.POSTGRES_HOST}...")
    try:
        conn = psycopg2.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            dbname=settings.POSTGRES_DB
        )
        cur = conn.cursor()
        
        # 1. УДАЛЕНИЕ СТАРЫХ ТАБЛИЦ (Hard Reset)
        print("🗑️ Dropping old tables...")
        # Сначала удаляем charts, так как она зависит от users
        cur.execute("DROP TABLE IF EXISTS user_charts CASCADE;")
        cur.execute("DROP TABLE IF EXISTS users CASCADE;")
        
        # 2. СОЗДАНИЕ ТАБЛИЦ
        print("🛠 Creating new tables...")
        cur.execute("""
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(20) NOT NULL DEFAULT 'user',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        cur.execute("""
            CREATE TABLE user_charts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                name VARCHAR(100) NOT NULL,
                code TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, name)
            );
        """)
        
        # 3. СОЗДАНИЕ ПОЛЬЗОВАТЕЛЕЙ
        print("👤 Creating users...")
        # Генерируем хеши "здесь и сейчас", чтобы они точно работали
        admin_hash = get_password_hash("admin")
        user_hash = get_password_hash("user")
        
        cur.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, 'admin')",
            ('admin', admin_hash)
        )
        cur.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, 'user')",
            ('user', user_hash)
        )

        conn.commit()
        cur.close()
        conn.close()
        print("✅ DATABASE RESET SUCCESSFUL! (Users 'admin' and 'user' created)")
        
    except Exception as e:
        print(f"❌ Migration FAILED: {e}")

if __name__ == "__main__":
    run_migration()