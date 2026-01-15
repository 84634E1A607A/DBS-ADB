Your overall goal is to create a simple database management system that has basic functionalities such as creating, reading, updating, and deleting records. The system should be able to handle user input and store data in a structured format.

You have a reference document `../dbs-tutorial/`, in which you can find useful information and examples to help you achieve your goal. Make sure to explore the document thoroughly and utilize the information provided to build this system.

The codebase is written in Rust in `.` directory. Before importing any external libraries, prompt me for approval.

Your testcases are located in `../dbs-testcase/` folder, check `../dbs-testcase/README.md` for instructions on how to run the testcases against your implementation. Your implementation is good, as long as it can pass relevant testcases.

Run `python3 runner.py -f query join pk index data fk comb aggregate order fuzzy -- /home/ajax/source/db/adb/target/release/adb` and make sure you pass the cases.

To debug, use `echo "QUERY HERE; CAN BE MULTIPLE;" | /home/ajax/source/db/adb/target/release/adb` to run single-line queries efficiently.

The testcases are not that complex, so focus on correctness and don't over-engineer the solution. After you finish the implementation, you should be able to pass some testcases.

## Source Code Structure

```text
src/
├── main.rs              # CLI interface and entry point
├── lib.rs               # Library exports
├── database/            # Database management
├── record/             # Record management, handle tables and records.
├── file/               # File management
│   ├── mod.rs          # Constants and exports
│   ├── file_manager.rs # PagedFileManager
│   ├── buffer_manager.rs # BufferManager, LRU cache
│   └── error.rs        # File errors
├── btree/              # B+ tree implementation
├── index/              # Index management
├── lexer_parser/       # SQL parsing
└── catalog/            # Metadata management
```
