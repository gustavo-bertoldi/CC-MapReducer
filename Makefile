CC = g++
CCFLAGS = -Wall -ansi -std=c++11

run: prepare mapper shuffler reducer
	./bin/mapper
	./bin/shuffler
	./bin/reducer

mapper: src/mapper.cpp
	$(CC) $(CCFLAGS) -o bin/mapper src/mapper.cpp

shuffler: src/shuffler.cpp
	$(CC) $(CCFLAGS) -o bin/shuffler src/shuffler.cpp

reducer: src/reducer.cpp
	$(CC) $(CCFLAGS) -o bin/reducer src/reducer.cpp

prepare: clean
	mkdir -p bin
	mkdir -p output

clean:
	@echo "Cleaning binaries..."
	@rm -r -f ./bin
	@rm -r -f ./output
