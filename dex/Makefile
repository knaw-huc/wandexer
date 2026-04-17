TARGET= a.out 

CC=     gcc
DEBUG=  -g -DDEBUG
DEFS=
CCFLAGS=$(DEFS) $(DEBUG)
INCL=   -I.
LIBS=	$(shell pkg-config --libs json-c)
WARN=	-Wall -Werror -O2
WARN+=	-Wstrict-prototypes -Wmissing-prototypes
WARN+=	-Wmissing-declarations
WARN+=	-Wshadow -Wpointer-arith -Wcast-qual
WARN+=	-Wsign-compare
CFLAGS= $(CCFLAGS) $(INCL) $(WARN)
CFLAGS+=$(shell pkg-config --cflags json-c)
LDFLAGS=

SRCS=   main.c
OBJS=	$(SRCS:.c=.o)

all: $(TARGET) tags

run: $(TARGET)
	time ./$(TARGET) doc.json
	time ./$(TARGET) let.json

tags: $(SRCS)
	ctags -R

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) $(OBJS) -o $(TARGET) $(LIBS)

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean run
