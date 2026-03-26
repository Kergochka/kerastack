import sqlite3
import sys
from pathlib import Path

# Allow running this file directly from the examples folder.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kerastack.KergaSQL import KColumn, KCoreORM, kregister


DB_PATH = "example.db"


@kregister
class UsersDB(KCoreORM):
    __slots__ = ()
    _table_name = "users"

    id = KColumn("INTEGER PRIMARY KEY AUTOINCREMENT")
    name = KColumn("TEXT")
    age = KColumn("INTEGER")

    def __init__(self, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        super().__init__(db, cursor)


def print_menu() -> None:
    print("\n=== CONNECTION ADMIN MENU ===")
    print("1) Add SQL to queue")
    print("2) Show queued SQL")
    print("3) Execute queued SQL")
    print("4) Execute SQL now")
    print("5) Drop column")
    print("6) Drop table")
    print("7) Commit")
    print("8) Rollback")
    print("0) Exit")


def run() -> None:
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()
    conn = UsersDB(db, cur)

    print(f"Connected to: {DB_PATH}")
    print("Note: this script uses CONNECTION mode operations only.")

    while True:
        print_menu()
        cmd = input("Choose action: ").strip()

        try:
            if cmd == "1":
                sql = input("SQL to queue: ").strip()
                conn.add_requests(sql)
                print("Queued.")

            elif cmd == "2":
                queue = conn.get_requests()
                if not queue:
                    print("Queue is empty.")
                else:
                    print("Queued SQL:")
                    for idx, item in enumerate(queue, start=1):
                        print(f"{idx}. {item}")

            elif cmd == "3":
                conn.execute("")
                print("Queued SQL executed (remember to commit if needed).")

            elif cmd == "4":
                sql = input("SQL to execute now: ").strip()
                conn.execute(sql)
                print("SQL executed (remember to commit if needed).")

            elif cmd == "5":
                col = input("Column to drop: ").strip()
                result = conn.drop_columns(col)
                print(result)

            elif cmd == "6":
                force = input("Drop table? (y/n): ").strip().lower()
                result = conn.drop(force)
                print(result)

            elif cmd == "7":
                db.commit()
                print("Committed.")

            elif cmd == "8":
                db.rollback()
                print("Rolled back.")

            elif cmd == "0":
                print("Bye.")
                break

            else:
                print("Unknown command.")

        except Exception as err:
            print(f"Error: {err}")

    db.close()


if __name__ == "__main__":
    run()
