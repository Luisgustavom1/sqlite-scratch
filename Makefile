sqlite: sqlite.c
	gcc ./sqlite.c -o sqlite

run: sqlite
	./sqlite
	
test: sqlite
	tests/bin/rspec tests/main.spec.rb
