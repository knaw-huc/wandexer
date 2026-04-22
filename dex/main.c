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

#define NELEMS(arr) ((int)(sizeof(arr) / sizeof((arr)[0])))

typedef struct json_object json_t;

// global indexing document root
static json_t *_root;

static json_t *
find_by_path(json_t *obj, const char *path)
{
    char *path_copy = strdup(path);
    char *token = strtok(path_copy, ".");
    json_t *current = obj;
    json_t *next = NULL;

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

static const char *
find_str(json_t *obj, const char *path)
{
    json_t *match = find_by_path(obj, path);
    return match ? json_object_get_string(match) : NULL;
}

static void
add_str(const char *key, const char *val)
{
    json_object_object_add(_root, key, json_object_new_string(val));
}

static inline
void index_fields(json_t *anno)
{
    static struct {
        const char *key;
        const char *path;
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

    for (int i = 0; i < NELEMS(fields); i++) {
        json_t *p;
        if ((p = find_by_path(anno, fields[i].path))) {
            json_t *ref = json_object_get(p);
            json_object_object_add(_root, fields[i].key, ref);
        }
    }
}

static inline
void index_id(json_t *anno)
{
    const char *p;

    if ((p = find_str(anno, "body.id")))
        add_str("_id", p);
}

static void
index_anno(json_t *anno)
{
    index_id(anno);
    index_fields(anno);
}

static void
add_unique(const char *key, const char *candidate)
{
    json_t *items;

    // 1. Get or create the items
    if (!json_object_object_get_ex(_root, key, &items)) {
        items = json_object_new_array();
        json_object_object_add(_root, key, items);
    }

    // 2. Check if the candidate already exists
    int nitems = json_object_array_length(items);
    int exists = 0;
    for (int i = 0; i < nitems; i++) {
        json_t *item = json_object_array_get_idx(items, i);
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
index_artwork(json_t *anno)
{
    json_t *ref;

    if ((ref = find_by_path(anno, "body.tei:ref"))) {
        json_t *res;

        if ((res = find_by_path(ref, "id")))
            add_unique("artworkIds", json_object_get_string(res));

        if ((res = find_by_path(ref, "label.en.search")))
            add_unique("artworksEN", json_object_get_string(res));
    }
}

static inline void
index_person_ref(json_t *ref, const char *anno_id)
{
    json_t *res;

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
index_person(json_t *anno)
{
    json_t *ref;

    if ((ref = find_by_path(anno, "body.tei:ref"))) {
        const char *anno_id = find_str(anno, "body.id");

        if (json_object_get_type(ref) == json_type_array) {
            int nitems = json_object_array_length(ref);
            for (int i=0; i < nitems; i++)
                index_person_ref(json_object_array_get_idx(ref, i), anno_id);
        }
        else
            index_person_ref(ref, anno_id);
    }
}

static json_t *
extract_text(const char *type, const char *source, int start, int end)
{
    json_t *text = NULL;

    const char *basename = strrchr(source, '/') + 1;
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
store_text(json_t *anno, const char *text_type)
{
    json_t *texts;
    if (!json_object_object_get_ex(_root, text_type, &texts)) {
        texts = json_object_new_array();
        json_object_object_add(_root, text_type, texts);
    }

    json_t *targets;
    if (json_object_object_get_ex(anno, "target", &targets)
            && json_object_get_type(targets) == json_type_array) {
        int nitems = json_object_array_length(targets);
        for (int i = 0; i < nitems; i++) {
            json_t *target = json_object_array_get_idx(targets, i);
            const char *type;
            if ((type = find_str(target, "type")) && !strcmp(type, "NormalText")) {
                json_t *source, *selector, *start, *end;
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
    json_t *const *obj1 = (json_t *const *)a;
    json_t *const *obj2 = (json_t *const *)b;

    const char *id1 = json_object_get_string(*obj1);
    const char *id2 = json_object_get_string(*obj2);

    return strcmp(id1, id2);
}

int
main(int argc, char *argv[])
{
    FILE *fp;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <file>\n", argv[0]);
        return 1;
    }

    // setup locale for unicode reading
    setlocale(LC_ALL, "en_US.UTF-8");

    // allocate global (static) indexing doc
    _root = json_object_new_object();

    if (!(fp = fopen(argv[1], "r"))) {
        perror(argv[1]);
        return 1;
    }

    char *line = NULL;
    size_t linecap = 0;
    for (int line_num = 1; getline(&line, &linecap, fp) != -1; line_num++) {
        // each line in jsonld file is one web annotation
        json_t *anno = json_tokener_parse(line);
        if (!anno) {
            fprintf(stderr, "Error parsing JSON on line %d\n", line_num);
            continue;
        }

        const char *body_type = find_str(anno, "body.type");
        if (body_type) {
            switch (body_type[0]) {
                case 'D':
                    if (!strcmp(body_type, "Document")) {
                        add_str("type", "intro");
                        index_anno(anno);
                    }
                    else if (!strcmp(body_type, "Division")) {
                        const char *tei_type = find_str(anno, "body.tei:type");
                        if (tei_type) {
                            if (!strcmp(tei_type, "original"))
                                store_text(anno, "letterOriginalText");
                            else if (!strcmp(tei_type, "translation"))
                                store_text(anno, "letterTranslatedText");
                            else if (!strcmp(tei_type, "about"))
                                store_text(anno, "introText");
                        }
                    }
                    break;

                case 'E':
                    if (!strcmp(body_type, "Entity")) {
                        const char *tei_type = find_str(anno, "body.tei:type");
                        if (tei_type) {
                            if (!strcmp(tei_type, "artwork"))
                                index_artwork(anno);
                            else if (!strcmp(tei_type, "person"))
                                index_person(anno);
                        }
                    }
                    break;

                case 'L':
                    if (!strcmp(body_type, "Letter")) {
                        add_str("type", "letter");
                        index_anno(anno);
                    }
                    break;

                case 'N':
                    if (!strcmp(body_type, "Note")) {
                        const char *sub_type = find_str(anno, "body.subtype");
                        if (sub_type
                                && (!strcmp(sub_type, "notes")
                                    || !strcmp(sub_type, "typednotes")
                                    || !strcmp(sub_type, "langnotes")))
                            store_text(anno, "letterNotesText");
                    }
                    break;

                // ignore
                default:
                    break;
            }
        }

        // dereference/free this iteration's anno
        json_object_put(anno);
    }

    // cleanup technically not needed as we're about to exit, but oh well.
    free(line);
    linecap = 0;

    const char *sortable_fields[] = {
        "artworkIds",
        "artworksEN",
        "persons"
    };

    for (int i = 0; i < NELEMS(sortable_fields); i++) {
        json_t *arr;
        if (json_object_object_get_ex(_root, sortable_fields[i], &arr))
            json_object_array_sort(arr, cmp_json_by_strval);
    }

    puts(json_object_to_json_string_ext(
                _root,
                JSON_C_TO_STRING_PRETTY | JSON_C_TO_STRING_NOSLASHESCAPE));

    return 0;
}
