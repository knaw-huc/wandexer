#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <wchar.h>
#include <locale.h>
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
add_unique(str key, str candidate)
{
    json *items;

    // 1. Get or create the items
    if (!json_object_object_get_ex(_index_doc, key, &items)) {
        items = json_object_new_array();
        json_object_object_add(_index_doc, key, items);
    }

    // 2. Check if the candidate already exists
    int nitems = json_object_array_length(items);
    int exists = 0;
    for (int i = 0; i < nitems; i++) {
        json *item = json_object_array_get_idx(items, i);
        if (!strcmp(json_object_get_string(item), candidate)) {
            exists = 1;
            break;
        }
    }

    // 3. Add the new string if it's unique
    if (!exists)
        json_object_array_add(items, json_object_new_string(candidate));
}

static void
index_artwork(json *anno)
{
    json *ref;

    if ((ref = find_by_path(anno, "body.tei:ref"))) {
        json *res;

        if ((res = find_by_path(ref, "id")))
            add_unique("artworkIds", json_object_get_string(res));

        if ((res = find_by_path(ref, "label.en.search")))
            add_unique("artworksEN", json_object_get_string(res));
    }
}

static inline void
index_person_ref(json *ref, str anno_id)
{
    json *res;

    if ((res = find_by_path(ref, "sortLabel")))
        add_unique("persons", json_object_get_string(res));
    else if ((res = find_by_path(ref, "displayLabel"))) {
        fprintf(stderr, "Using 'displayLabel' in lieu of 'sortLabel' in %s\n", anno_id);
        add_unique("persons", json_object_get_string(res));
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

static json *
extract_text(str type, str source, int start, int end)
{
    json *text = NULL;

    str basename = strrchr(source, '/') + 1;
    char *filename;
    if (asprintf(&filename, "%s.txt", basename) != -1) {
        FILE *fp = fopen(filename, "r");
        if (fp) {
            int num_codepoints = end - start;
            int worst_case_size = MB_CUR_MAX * num_codepoints + 1; // +1 for '\0'
            char *buf = malloc(worst_case_size);
            if (buf) {
                char *p = buf;

                // first, skip to 'start'
                long todo = start;
                while (todo-- > 0)
                    if (fgetwc(fp) == WEOF)
                        break;

                // then, copy till 'end'
                todo = end - start;
                p = buf;
                while (todo-- > 0) {
                    wint_t wc = fgetwc(fp);
                    if (wc == WEOF)
                        break;
                    else {
                        char mb[MB_CUR_MAX];
                        int nr = wcrtomb(mb, (wchar_t)wc, NULL);
                        if (nr > 0) {
                            for (int i = 0; i < nr; i++)
                                *p++ = mb[i];
                        }
                    }
                }
                *p = '\0';
                text = json_object_new_string_len(buf, p - buf);
                free(buf);
            }
            fclose(fp);
        }

        if (!text) {
            char errmsg[BUFSIZ];
            snprintf(errmsg, BUFSIZ, "%s[%d..%d]: %s", filename, start, end, strerror(errno));
            perror(errmsg);
            text = json_object_new_string(errmsg);
        }

        free(filename);
    }

    return text;
}

static void
store_text(json *anno, str text_type)
{
    json *texts;
    if (!json_object_object_get_ex(_index_doc, text_type, &texts)) {
        texts = json_object_new_array();
        json_object_object_add(_index_doc, text_type, texts);
    }

    json *targets;
    if (json_object_object_get_ex(anno, "target", &targets)
            && json_object_get_type(targets) == json_type_array) {
        int nitems = json_object_array_length(targets);
        for (int i = 0; i < nitems; i++) {
            json *target = json_object_array_get_idx(targets, i);
            str type;
            if ((type = find_str(target, "type")) && !strcmp(type, "NormalText")) {
                json *source, *selector, *start, *end;
                if (json_object_object_get_ex(target, "source", &source)
                        && json_object_object_get_ex(target, "selector", &selector)
                        && json_object_object_get_ex(selector, "start", &start)
                        && json_object_object_get_ex(selector, "end", &end))
                {
                    json_object_array_add(texts,
                            extract_text(
                                text_type,
                                json_object_get_string(source),
                                json_object_get_int(start),
                                json_object_get_int(end)));
                }
            }
        }
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
    int fd, line_num;
    off_t size;
    struct stat st;
    char *file, *line;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <file>\n", argv[0]);
        return 1;
    }

    setlocale(LC_ALL, "en_US.UTF-8");

    _index_doc = json_object_new_object();

    fd = open(argv[1], O_RDONLY);
    fstat(fd, &st);
    size = st.st_size;
    fprintf(stderr, "indexing \"%s\", size=%lld\n", argv[1], size);

    line = file = (char *) mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd, 0);
    line_num = 1;
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
                else if (!strcmp(body_type, "Division")) {
                    str tei_type = find_str(root, "body.tei:type");
                    if (tei_type) {
                        if (!strcmp(tei_type, "original"))
                            store_text(root, "letterOriginalText");
                        else if (!strcmp(tei_type, "translation"))
                            store_text(root, "letterTranslatedText");
                        else if (!strcmp(tei_type, "about"))
                            store_text(root, "introText");
                    }
                }
                else if (!strcmp(body_type, "Note")) {
                    str sub_type = find_str(root, "body.subtype");
                    if (sub_type
                            && (!strcmp(sub_type, "notes")
                                || !strcmp(sub_type, "typednotes")
                                || !strcmp(sub_type, "langnotes")))
                        store_text(root, "letterNotesText");
                }
            }

            line = &file[i+1];
            line_num++;

            json_object_put(root);
        }
    }

    json *res;
    if (json_object_object_get_ex(_index_doc, "artworkIds", &res))
        json_object_array_sort(res, cmp_json_by_strval);
    if (json_object_object_get_ex(_index_doc, "artworksEN", &res))
        json_object_array_sort(res, cmp_json_by_strval);
    if (json_object_object_get_ex(_index_doc, "persons", &res))
        json_object_array_sort(res, cmp_json_by_strval);

    str idx_txt = json_object_to_json_string_ext(_index_doc,
            JSON_C_TO_STRING_PRETTY | JSON_C_TO_STRING_NOSLASHESCAPE);
    puts(idx_txt);

    return 0;
}
