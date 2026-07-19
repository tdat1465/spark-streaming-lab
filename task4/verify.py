import json
from neo4j import GraphDatabase

NEO4J_URI  = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password123"


if __name__ == "__main__":
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASS)
    )

    try:
        with driver.session() as s:

            # Đếm tổng số node
            total_nodes = s.run(
                "MATCH (n:CpgNode) RETURN count(n) AS c"
            ).single()["c"]
            print(f"Tổng số CpgNode  : {total_nodes:,}")

            # Đếm tổng số relationship
            total_edges = s.run(
                "MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS c"
            ).single()["c"]
            print(f"Tổng số CPG_EDGE : {total_edges:,}")

            # Thống kê các loại node
            print("\nThống kê loại node (Top 10):")

            rows = s.run("""
                MATCH (n:CpgNode)
                RETURN n.type AS t, count(n) AS c
                ORDER BY c DESC
                LIMIT 10
            """)

            for r in rows:
                print(f"  {r['t']:<28} {r['c']:,}")

            # Thống kê các loại relationship
            print("\nThống kê loại relationship:")

            rows = s.run("""
                MATCH ()-[r:CPG_EDGE]->()
                RETURN r.type AS t, count(r) AS c
                ORDER BY c DESC
            """)

            for r in rows:
                print(f"  {r['t']:<28} {r['c']:,}")

            # Danh sách các tệp đã xử lý
            print("\nCác tệp đã xử lý:")

            rows = s.run("""
                MATCH (n:CpgNode)
                RETURN DISTINCT n.file_path AS f, count(n) AS c
                ORDER BY c DESC
            """)

            for r in rows:
                print(f"  {r['f']}  ->  {r['c']:,} node")

            # Hiển thị một node mẫu
            print("\nNode mẫu:")

            rec = s.run(
                "MATCH (n:CpgNode) RETURN n LIMIT 1"
            ).single()

            if rec:
                print(json.dumps(dict(rec["n"]), indent=2, default=str))

            # Kiểm tra tính idempotent (merge không tạo node trùng)
            dupes = s.run("""
                MATCH (n:CpgNode)
                WITH n.id AS id, count(*) AS c
                WHERE c > 1
                RETURN count(*) AS dupes
            """).single()["dupes"]

            print(f"\nKiểm tra idempotent (MERGE): {dupes} node trùng")

            if dupes == 0:
                print("-> Không có node trùng, MERGE hoạt động đúng.")

    finally:
        driver.close()