package pgb

import "testing"

type InsertEntity struct {
	A string `db:"a"`
	B string `db:"b"`
	C string `db:"c"`
	D string `db:"d"`
}

var insertEntities = []InsertEntity{
	{A: "a1", B: "b1", C: "c1"},
	{A: "a2", B: "b2", C: "c2"},
	{A: "a3", B: "b3", C: "c3"},
}

var doUpdate = map[string]any{
	"b":          Excluded("b"),
	"updated_at": SQLValue("now()"),
	"d":          1,
	"e":          nil,
}

func testBuilder() *InsertBuilder[InsertEntity] {
	return NewInsertBuilder("public.test",
		insertEntities,
		func(v InsertEntity) map[string]any {
			return map[string]any{"a": v.A, "b": v.B, "c": SQLValue("now()")}
		},
	).OnConflictDoUpdate("constraint_name", doUpdate).Returning("b", "c")
}

func TestInsertBuilder_Sql(t *testing.T) {
	b := testBuilder()

	expected := "INSERT INTO \"public\".\"test\" (\"a\", \"b\", \"c\") VALUES ('a1', 'b1', now()), ('a2', 'b2', now()), ('a3', 'b3', now()) ON CONFLICT ON CONSTRAINT \"constraint_name\" DO UPDATE SET \"b\" = EXCLUDED.\"b\", \"d\" = 1, \"e\" = NULL, \"updated_at\" = now() RETURNING \"b\", \"c\""
	if b.RawSql() != expected {
		t.Errorf("Unexpected sql statement: %s", b.RawSql())
	}
}

func BenchmarkInsertBuilder_Build(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		testBuilder().Build()
	}
}
