CC = g++
CCFLAGS = -Wall -ansi -std=c++11
BIN := mapper shuffler reducer

run: prepare $(BIN)
	./bin/mapper
	./bin/shuffler
	./bin/reducer

$(BIN): %: src/%.cpp
	$(CC) $(CCFLAGS) -o bin/$@ $<  

prepare: clean
	@mkdir -p bin
	@mkdir -p output

clean:
	@echo "Cleaning binaries..."
	@rm -r -f ./bin
	@rm -r -f ./output
