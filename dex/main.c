#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <json-c/json.h>

typedef const char *str;
typedef struct json_object json;

static json *_index_doc;

static json *
find_by_path(json *obj, str path)
{
    char *path_copy = strdup(path);
    char *token = strtok(path_copy, ".");
    json *current = obj;
    json *next = NULL;

    while (token != NULL) {
        if (json_object_object_get_ex(current, token, &next)) {
            current = next;
        } else {
            free(path_copy);
            return NULL;
        }
        token = strtok(NULL, ".");
    }

    free(path_copy);
    return current;
}

static str
find_str(json *obj, str path)
{
    json *match = find_by_path(obj, path);
    return match ? json_object_get_string(match) : NULL;
}

static void
add_str(str key, str val)
{
    json_object_object_add(_index_doc, key, json_object_new_string(val));
}

static inline
void index_fields(json *anno)
{
    static struct {
        str key;
        str path;
    } fields[] = {
        { "recipient",      "body.recipient" },
        { "file",           "body.n" },
        { "location",       "body.location" },
        { "msId",           "body.identifier" },
        { "sender",         "body.sender" },
        { "title",          "body.title" },
        { "correspondent",  "body.participant" },
        { "period",         "body.namedPeriod" },
    };

    int nelems = sizeof(fields) / sizeof(fields[0]);

    for (int i = 0; i < nelems; i++) {
        json *p;
        if ((p = find_by_path(anno, fields[i].path))) {
            json *ref = json_object_get(p);
            json_object_object_add(_index_doc, fields[i].key, ref);
        }
    }
}

static inline
void index_id(json *anno)
{
    const char *p;

    if ((p = find_str(anno, "body.id")))
        add_str("_id", p);
}

static void
index_anno(json *anno)
{
    index_id(anno);
    index_fields(anno);
}

static void
add_artwork_id(str id)
{
    json *ids;

    // setup array if we don't have it yet
    if (!json_object_object_get_ex(_index_doc, "artworkIds", &ids)) {
        ids = json_object_new_array();
        json_object_object_add(_index_doc, "artworkIds", ids);
    }

    // only add artwork_id if it isn't already in our list
    int nitems = json_object_array_length(ids);
    int exists = 0;
    for (int i = 0; i < nitems; i++) {
        json *item = json_object_array_get_idx(ids, i);
        if (!strcmp(json_object_get_string(item), id)) {
            exists = 1;
            break;
        }
    }

    if (!exists)
        json_object_array_add(ids, json_object_new_string(id));
}

static void
add_artwork_label(str label)
{
    json *labels;

    if (!json_object_object_get_ex(_index_doc, "artworksEN", &labels)) {
        labels = json_object_new_array();
        json_object_object_add(_index_doc, "artworksEN", labels);
    }

    int nitems = json_object_array_length(labels);
    int exists = 0;
    for (int i = 0; i < nitems; i++) {
        json *item = json_object_array_get_idx(labels, i);
        if (!strcmp(json_object_get_string(item), label)) {
            exists = 1;
            break;
        }
    }

    if (!exists)
        json_object_array_add(labels, json_object_new_string(label));
}

static void
index_artwork(json *anno)
{
    json *ref;

    if ((ref = find_by_path(anno, "body.tei:ref"))) {
        json *res;

        if ((res = find_by_path(ref, "id")))
            add_artwork_id(json_object_get_string(res));

        if ((res = find_by_path(ref, "label.en.search")))
            add_artwork_label(json_object_get_string(res));
    }
}

static void
add_person(str label)
{
    json *labels;

    if (!json_object_object_get_ex(_index_doc, "persons", &labels)) {
        labels = json_object_new_array();
        json_object_object_add(_index_doc, "persons", labels);
    }

    int nitems = json_object_array_length(labels);
    int exists = 0;
    for (int i = 0; i < nitems; i++) {
        json *item = json_object_array_get_idx(labels, i);
        if (!strcmp(json_object_get_string(item), label)) {
            exists = 1;
            break;
        }
    }

    if (!exists)
        json_object_array_add(labels, json_object_new_string(label));
}

static inline void
index_person_ref(json *ref, str anno_id)
{
    json *res;

    if ((res = find_by_path(ref, "sortLabel")))
        add_person(json_object_get_string(res));
    else if ((res = find_by_path(ref, "displayLabel"))) {
        fprintf(stderr, "Using 'displayLabel' in lieu of 'sortLabel' in %s\n", anno_id);
        add_person(json_object_get_string(res));
    }
    else
        fprintf(stderr, "Missing 'sortLabel' and 'displayLabel' in %s\n", anno_id);
}

static void
index_person(json *anno)
{
    json *ref;

    if ((ref = find_by_path(anno, "body.tei:ref"))) {
        str anno_id = find_str(anno, "body.id");

        if (json_object_get_type(ref) == json_type_array) {
            int nitems = json_object_array_length(ref);
            for (int i=0; i < nitems; i++)
                index_person_ref(json_object_array_get_idx(ref, i), anno_id);
        }
        else
            index_person_ref(ref, anno_id);
    }
}

static int
cmp_json_by_strval(const void *a, const void *b)
{
    json *const *obj1 = (json *const *)a;
    json *const *obj2 = (json *const *)b;

    const char *id1 = json_object_get_string(*obj1);
    const char *id2 = json_object_get_string(*obj2);

    return strcmp(id1, id2);
}

int
main(int argc, char *argv[])
{
    int fd, line_num, skipped;
    off_t size;
    struct stat st;
    char *file, *line;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <file>\n", argv[0]);
        return 1;
    }

    _index_doc = json_object_new_object();

    fd = open(argv[1], O_RDONLY);
    fstat(fd, &st);
    size = st.st_size;
    printf("%s, size=%lld\n", argv[1], size);

    line = file = (char *) mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd, 0);
    line_num = 1;
    skipped = 0;
    for (off_t i = 0; i < size; i++) {
        if (file[i] == '\n') {
            file[i] = '\0';
            json *root = json_tokener_parse(line);
            if (!root) {
                fprintf(stderr, "Error parsing JSON on line %d\n", line_num);
                continue;
            }

            str body_type = find_str(root, "body.type");
            if (body_type) {
                if (!strcmp(body_type, "Letter")) {
                    add_str("type", "letter");
                    index_anno(root);
                }
                else if (!strcmp(body_type, "Document")) {
                    add_str("type", "intro");
                    index_anno(root);
                }
                else if (!strcmp(body_type, "Entity")) {
                    str tei_type = find_str(root, "body.tei:type");
                    if (tei_type) {
                        if (!strcmp(tei_type, "artwork"))
                            index_artwork(root);
                        else if (!strcmp(tei_type, "person"))
                            index_person(root);
                    }
                }
                else
                    skipped++;
            }

            line = &file[i+1];
            line_num++;

            json_object_put(root);
        }
    }

    fprintf(stderr, "Skipped %d out of %d lines\n", skipped, line_num - 1);

    json *res;
    if (json_object_object_get_ex(_index_doc, "artworkIds", &res))
        json_object_array_sort(res, cmp_json_by_strval);
    if (json_object_object_get_ex(_index_doc, "artworksEN", &res))
        json_object_array_sort(res, cmp_json_by_strval);
    if (json_object_object_get_ex(_index_doc, "persons", &res))
        json_object_array_sort(res, cmp_json_by_strval);

    str idx_txt = json_object_to_json_string_ext(_index_doc,
            JSON_C_TO_STRING_PRETTY | JSON_C_TO_STRING_NOSLASHESCAPE);
    fprintf(stderr, "%s\n", idx_txt);

    return 0;
}
