# GoDB - Go 语言实现的嵌入式 SQL 数据库

GoDB 是一个用 Go 语言实现的嵌入式 SQL 数据库，从 RusticDB 改写而来。支持 MVCC、事务隔离、多种存储引擎。

## 我是怎么做的？
我使用了 Claude Code 搭配GLM-5，并且搭配了插件oh-my-claudecode，使用了`/ralph`命令去让cc持续运行，之后手动测试一些边界条件并修复bugs。

针对重写的需求，我认为本来的项目要有足够好的测试文件代码才可以一口气全部进行重写，有了好的测试代码会让 AI 更加有的放矢地去重写代码，不过最终还是需要自己去考虑一些边界条件，针对数据库，还是要实际跑起来，插入一些数据才知道有没有问题。

如果项目没有测试文件，我建议从git第一次提交开始，逐次改写（循序渐进的效果）。

## 目录

- [项目架构](#项目架构)
- [从 Rust 到 Go 的改写指南](#从-rust-到-go-的改写指南)
- [快速开始](#快速开始)
- [测试说明](#测试说明)
- [清理项目](#清理项目)

---

## 项目架构

### 目录结构

```
goDB/
├── cmd/                          # 应用入口
│   ├── server/main.go            # TCP 服务端 (端口 8080)
│   └── client/main.go            # 命令行客户端 (REPL)
├── internal/                     # 内部包
│   ├── dberror/error.go          # 错误类型定义
│   ├── storage/                  # 存储层
│   │   ├── engine.go             # Engine 接口定义
│   │   ├── memory.go             # 内存存储引擎
│   │   ├── disk.go               # 磁盘存储引擎 (Bitcask 风格)
│   │   ├── keycode.go            # 键编码器 (有序编码)
│   │   ├── mvcc.go               # MVCC 并发控制实现
│   │   └── *_test.go             # 存储层测试
│   └── sql/
│       ├── types/                # 类型系统
│       │   ├── types.go          # SQL 值类型
│       │   └── schema.go         # 表结构定义
│       ├── parser/               # SQL 解析器
│       │   ├── lexer.go          # 词法分析
│       │   ├── parser.go         # 语法分析
│       │   └── ast.go            # AST 节点定义
│       ├── plan/                 # 查询计划
│       │   ├── planner.go        # 计划生成器
│       │   └── node.go           # 执行计划节点
│       ├── executor/             # 执行器
│       │   ├── executor.go       # 执行入口
│       │   ├── query.go          # 查询执行
│       │   ├── mutation.go       # INSERT/UPDATE/DELETE
│       │   ├── join.go           # JOIN 执行
│       │   └── agg.go            # 聚合函数
│       └── engine/               # SQL 引擎
│           ├── kv.go             # KV 存储适配
│           └── session.go        # 会话管理
├── go.mod                        # Go 模块定义
└── go.sum                        # 依赖校验
```

### 模块依赖关系

```
┌─────────────────────────────────────────────────────────────────────┐
│                          cmd/server & cmd/client                     │
│                        (应用入口，网络通信)                           │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────┐
│                        internal/sql/executor                         │
│    (执行计划节点：Scan, Filter, Join, Aggregation, Mutation)         │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────┐
│                         internal/sql/plan                            │
│               (查询计划生成：AST -> Plan Node)                        │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────┐
│                        internal/sql/parser                           │
│              (SQL 解析：Lexer + Parser -> AST)                       │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────┐
│                        internal/sql/engine                           │
│          (SQL 引擎：Session 管理事务，KVEngine 操作存储)              │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────┐
│                        internal/storage                              │
│   (存储层：Engine 接口，MemoryEngine/DiskEngine，MVCC 事务)          │
└─────────────────────────────────────────────────────────────────────┘
```

### 核心数据结构

| 结构体 | 文件位置 | 职责 |
|--------|----------|------|
| `Engine` | `internal/storage/engine.go` | 存储引擎接口 |
| `MemoryEngine` | `internal/storage/memory.go` | 内存存储实现 |
| `DiskEngine` | `internal/storage/disk.go` | 磁盘存储实现 |
| `Mvcc` | `internal/storage/mvcc.go` | MVCC 层，管理版本号和活跃事务 |
| `MvccTransaction` | `internal/storage/mvcc.go` | 事务对象，包含读写集和可见性快照 |
| `KVEngine` | `internal/sql/engine/kv.go` | SQL 到 KV 的映射层 |
| `Session` | `internal/sql/engine/session.go` | 客户端会话，事务隔离 |
| `Value` | `internal/sql/types/types.go` | SQL 值 (支持 NULL/Bool/Int/Float/String) |
| `Table` | `internal/sql/types/schema.go` | 表结构定义 |

---

## 从 Rust 到 Go 的改写指南

### 1. 类型系统转换

#### 枚举类型

**Rust:**
```rust
enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}
```

**Go:**
```go
type DataType int

const (
    TypeBoolean DataType = iota
    TypeInteger
    TypeFloat
    TypeString
)
```

#### 联合类型 (Value)

**Rust:**
```rust
enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}
```

**Go (使用结构体包含所有字段):**
```go
type Value struct {
    Type  DataType  // 类型标记
    Null  bool      // NULL 标记
    Bool  bool
    Int   int64
    Float float64
    Str   string
}
```

### 2. Option 和 Result 类型

#### Option<T> 转换

**Rust:**
```rust
fn get(&self, key: &[u8]) -> Option<&[u8]>
```

**Go (使用指针 + nil 或多返回值):**
```go
func (e *Engine) Get(key []byte) ([]byte, error)
// 返回 (nil, nil) 表示不存在
// 返回 (value, nil) 表示找到
// 返回 (nil, error) 表示错误
```

#### Result<T, E> 转换

**Rust:**
```rust
fn set(&self, key: &[u8], value: &[u8]) -> Result<(), Error>
```

**Go:**
```go
func (e *Engine) Set(key, value []byte) error
// 返回 nil 表示成功
// 返回 error 表示失败
```

### 3. 错误处理

**Rust (? 运算符):**
```rust
fn operation() -> Result<(), Error> {
    let value = some_func()?;  // 自动传播错误
    another_func(value)?;
    Ok(())
}
```

**Go (显式错误检查):**
```go
func operation() error {
    value, err := someFunc()
    if err != nil {
        return err  // 手动传播错误
    }
    if err := anotherFunc(value); err != nil {
        return err
    }
    return nil
}
```

### 4. 并发模型

#### 锁的使用

**Rust (Mutex/RwLock 在类型系统内):**
```rust
struct MemoryEngine {
    data: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryEngine {
    fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut data = self.data.write().unwrap();
        data.insert(key, value);
    }
}
```

**Go (显式锁调用):**
```go
type MemoryEngine struct {
    mu   sync.RWMutex
    data map[string][]byte
}

func (e *MemoryEngine) Set(key, value []byte) error {
    e.mu.Lock()
    defer e.mu.Unlock()  // 确保释放锁
    e.data[string(key)] = value
    return nil
}
```

### 5. 迭代器模式

**Rust (Iterator trait):**
```rust
for item in iter {
    // ...
}
```

**Go (接口模式):**
```go
type EngineIterator interface {
    Next() bool
    Key() []byte
    Value() []byte
    Err() error
    Close()
}

// 使用模式:
it := engine.Scan(start, end)
for it.Next() {
    key := it.Key()
    val := it.Value()
    // ...
}
it.Close()  // 必须手动关闭
```

### 6. 泛型处理

**Rust:**
```rust
trait Engine {
    fn scan<'a>(&'a self, start: &[u8], end: &[u8]) -> Box<dyn Iterator<Item=(&'a [u8], &'a [u8])> + 'a>;
}
```

**Go (使用接口替代泛型):**
```go
type Engine interface {
    Scan(start, end []byte) EngineIterator
    ScanPrefix(prefix []byte) EngineIterator
}
```

### 7. JSON 序列化

**Rust (Serde):**
```rust
#[derive(Serialize, Deserialize)]
struct Table {
    name: String,
    columns: Vec<Column>,
}
```

**Go (encoding/json):**
```go
type Table struct {
    Name    string   `json:"name"`
    Columns []Column `json:"columns"`
}

func (t *Table) Serialize() ([]byte, error) {
    return json.Marshal(t)
}

func (t *Table) Deserialize(data []byte) error {
    return json.Unmarshal(data, t)
}
```

### 8. 改写注意事项

1. **所有权和生命周期**: Go 没有 Rust 的所有权系统，依赖 GC 管理内存。需要注意避免循环引用和不必要的内存泄漏。

2. **并发安全**: Go 没有编译时的借用检查器，需要手动使用 `sync.Mutex` 或 `sync.RWMutex` 保护共享数据。

3. **空值处理**: Go 有 `nil`，但使用方式与 Rust 的 `Option` 不同。需要显式检查。

4. **错误处理**: Go 使用多返回值 `(T, error)`，需要显式检查每个错误。

5. **defer**: Go 的 `defer` 类似 Rust 的 RAII，用于确保资源释放。

6. **goroutine vs async/await**: Go 使用 goroutine 进行并发，不同于 Rust 的 async/await 模型。

---

## 快速开始

### 环境要求

- Go 1.21 或更高版本

### 依赖

项目唯一的外部依赖是 `readline`，用于客户端命令行交互：

```bash
go mod download
```

### 编译

```bash
# 编译服务端
go build -o bin/server ./cmd/server

# 编译客户端
go build -o bin/client ./cmd/client

# 或者同时编译
go build -o bin/server ./cmd/server && go build -o bin/client ./cmd/client
```

### 运行

#### 启动服务端

```bash
# 内存模式 (数据不持久化)
./bin/server :memory:

# 磁盘模式 (数据持久化到 ./data 目录)
./bin/server ./data

# 自定义数据目录
./bin/server /path/to/data
```

服务端默认监听 TCP 端口 8080。

#### 启动客户端

```bash
./bin/client
```

### SQL 示例

```sql
-- 创建表
godb> CREATE TABLE users (id int primary key, name string, age int);

-- 插入数据
godb> INSERT INTO users VALUES (1, 'Alice', 30);
godb> INSERT INTO users VALUES (2, 'Bob', 25);

-- 查询
godb> SELECT * FROM users;
id | name  | age
---+-------+-----
1  | Alice | 30
2  | Bob   | 25
(2 rows)

-- 条件查询
godb> SELECT * FROM users WHERE age > 26;

-- 更新
godb> UPDATE users SET age = 31 WHERE id = 1;

-- 删除
godb> DELETE FROM users WHERE id = 2;

-- 事务
godb> BEGIN;
godb#1> INSERT INTO users VALUES (3, 'Charlie', 35);
godb#1> COMMIT;

-- 查看表
godb> SHOW TABLES;
godb> SHOW TABLE users;
```

---

## 测试说明

### 测试文件组织

| 测试文件 | 测试内容 |
|----------|----------|
| `internal/storage/engine_test.go` | 存储引擎基础操作 |
| `internal/storage/keycode_test.go` | 键编码/解码 |
| `internal/storage/disk_test.go` | 磁盘存储持久化 |
| `internal/storage/mvcc_test.go` | MVCC 事务隔离 (1000+ 行) |
| `internal/sql/parser/lexer_test.go` | 词法分析 |
| `internal/sql/parser/parser_test.go` | 语法分析 |
| `internal/sql/plan/plan_test.go` | 查询计划生成 |
| `internal/sql/executor/mutation_test.go` | INSERT/UPDATE/DELETE |
| `internal/sql/executor/integration_test.go` | 端到端集成测试 |
| `internal/sql/engine/kv_test.go` | KV 引擎测试 |
| `internal/sql/engine/session_test.go` | 会话事务测试 |

### 运行测试

```bash
# 运行所有测试
go test ./...

# 运行特定包测试 (详细输出)
go test -v ./internal/storage/...

# 运行特定测试
go test -v -run TestDirtyReadPrevention ./internal/storage/

# 运行 MVCC 事务测试
go test -v ./internal/storage/ -run "TestDirtyRead|TestRollback|TestWriteConflict"

# 运行集成测试
go test -v ./internal/sql/executor/ -run Integration

# 查看测试覆盖率
go test -cover ./...
```

### MVCC 测试覆盖的场景

`internal/storage/mvcc_test.go` 是最全面的测试文件，覆盖以下场景：

1. **基本操作**: Get, Set, Delete
2. **读隔离**: 快照隔离验证
3. **脏读预防**: 未提交数据不可见
4. **不可重复读预防**: 同一事务内一致性读取
5. **幻读预防**: Scan 操作的一致性
6. **写冲突检测**: 并发写入冲突
7. **删除冲突检测**: 删除操作冲突
8. **回滚**: 事务回滚正确性
9. **扫描可见性**: MVCC 可见性规则
10. **版本持久化**: 重启后版本恢复

### 集成测试场景

`internal/sql/executor/integration_test.go` 覆盖：

1. **多行 INSERT 原子性**: `INSERT INTO tbl VALUES (1, 1.1), (1, 2.2);` 失败时不留下部分数据
2. **事务内多行 INSERT**: 失败的 INSERT 不影响后续操作
3. **语句内重复键检测**: 同一 INSERT 语句内的重复键检测

---

## 支持的 SQL 语法

### DDL (数据定义语言)

```sql
CREATE TABLE table_name (
    column1 datatype [PRIMARY KEY],
    column2 datatype,
    ...
);
```

### DML (数据操作语言)

```sql
-- 插入
INSERT INTO table_name VALUES (value1, value2, ...);
INSERT INTO table_name (col1, col2) VALUES (val1, val2), (val3, val4);

-- 查询
SELECT * FROM table_name;
SELECT col1, col2 FROM table_name WHERE condition;
SELECT * FROM table1 JOIN table2 ON table1.id = table2.id;
SELECT COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col) FROM table_name;

-- 更新
UPDATE table_name SET col1 = val1 WHERE condition;

-- 删除
DELETE FROM table_name WHERE condition;
```

### 事务控制

```sql
BEGIN;
-- SQL statements
COMMIT;
-- or
ROLLBACK;
```

### 其他

```sql
SHOW TABLES;
SHOW TABLE table_name;
```

---

## 数据类型

| SQL 类型 | Go 类型 | 说明 |
|----------|---------|------|
| `int` | `int64` | 整数 |
| `float` | `float64` | 浮点数 |
| `string` | `string` | 字符串 |
| `bool` | `bool` | 布尔值 |

---

## MVCC 实现说明

### 键空间设计

```
0x00 + 表名              → 表元数据
0x01 + 表名 + 主键       → 行数据 (用户键)
0x03 + 表名 + 主键 + 版本 → MVCC 版本化键
0x05 + 版本号            → 活跃事务标记
0x06                     → 下一个版本计数器
0x07 + 版本号 + 键       → 事务写标记
```

### 可见性规则

```go
func (t *MvccTransaction) isVisible(version uint64) bool {
    // 如果创建此版本的事务在当前事务开始时是活跃的，则不可见
    if t.activeTxns[version] {
        return false
    }
    // 快照隔离: 只能看到版本号 <= 自己版本号的数据
    return version <= t.version
}
```

### 事务隔离级别

- **快照隔离**: 每个事务看到的是事务开始时的数据快照
- **防止脏读**: 未提交的数据对其他事务不可见
- **写冲突检测**: 并发写入同一键时检测冲突

---

## License

MIT License
