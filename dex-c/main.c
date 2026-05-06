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

static inline int
streq(const char *a, const char *b)
{
    return strcmp(a, b) == 0;
}

static inline void
add_obj(const char *key, json_t *val)
{
    json_object_object_add(_root, key, val);
}

static inline void
add_str(const char *key, const char *val)
{
    add_obj(key, json_object_new_string(val));
}

static inline json_t *
get_obj(const char *key)
{
    return json_object_object_get(_root, key);
}

static json_t *
contrive_date(json_t *body)
{
    json_t *date = json_object_new_object();

    json_t *actual = json_object_object_get(body, "dateSent");
    json_t *not_before = json_object_object_get(body, "dateSentNotBefore");
    json_t *not_after = json_object_object_get(body, "dateSentNotAfter");

    if (actual) {
        json_object_object_add(date, "gte", json_object_get(actual));
        json_object_object_add(date, "lte", json_object_get(actual));
        if (not_before)
            fprintf(stderr, "%s: has both actual date AND notBefore!\n",
                    json_object_get_string(json_object_object_get(body, "id")));
        if (not_after)
            fprintf(stderr, "%s: has both actual date AND notAfter!\n",
                    json_object_get_string(json_object_object_get(body, "id")));
    }
    else {
        if (not_before)
            json_object_object_add(date, "gte", json_object_get(not_before));
        if (not_after)
            json_object_object_add(date, "lte", json_object_get(not_after));
    }

    if (json_object_object_length(date) > 0)
        return date;

    json_object_put(date);
    return NULL;
}

static void
index_anno(json_t *body)
{
    static struct {
        const char *key;
        const char *path;
    } fields[] = {
        { "recipient",      "recipient" },
        { "file",           "n" },
        { "location",       "location" },
        { "msId",           "identifier" },
        { "sender",         "sender" },
        { "title",          "title" },
        { "correspondent",  "participant" },
        { "period",         "namedPeriod" },
    };

    json_t *id = json_object_object_get(body, "id");
    if (id)
        add_str("id", json_object_get_string(id));

    for (int i = 0; i < NELEMS(fields); i++) {
        json_t *field = json_object_object_get(body, fields[i].path);
        if (field)
            add_obj(fields[i].key, json_object_get(field));
    }

    json_t *date = contrive_date(body);
    if (date) {
        json_t *gte = json_object_object_get(date, "gte");
        json_t *lte = json_object_object_get(date, "lte");
        if (gte)
            add_obj("dateSortable", json_object_get(gte));
        else if (lte)
            add_obj("dateSortable", json_object_get(lte));

        add_obj("date", date);
    }
    else {
        fprintf(stderr, "%s: no dateSent, winging it.\n", json_object_get_string(id));

        // improvise: 0001 <= 'date' <= 9999
        json_t *impr = json_object_new_object();
        json_object_object_add(impr, "gte", json_object_new_string("0001"));
        json_object_object_add(impr, "lte", json_object_new_string("9999"));
        add_obj("date", impr);

        // arbitrary 'sortable date' to come after items with proper (historical) date
        add_str("dateSortable", "9999");
    }

}

static void
add_unique(const char *key, const char *candidate)
{
    // Get or create and add items array
    json_t *items = get_obj(key);
    if (!items) {
        items = json_object_new_array();
        add_obj(key, items);
    }

    // Check if the candidate already exists
    int nitems = json_object_array_length(items);
    int exists = 0;
    for (int i = 0; i < nitems; i++) {
        json_t *item = json_object_array_get_idx(items, i);
        if (streq(json_object_get_string(item), candidate)) {
            exists = 1;
            break;
        }
    }

    // Add candidate if we don't have it yet
    if (!exists)
        json_object_array_add(items, json_object_new_string(candidate));
}

static void
index_artwork(json_t *body)
{
    json_t *ref = json_object_object_get(body, "tei:ref");

    if (ref) {
        json_t *res;

        if (json_object_object_get_ex(ref, "id", &res))
            add_unique("artworkIds", json_object_get_string(res));

        if ((res = find_by_path(ref, "label.en.search")))
            add_unique("artworksEN", json_object_get_string(res));
    }
}

static inline void
index_person_ref(json_t *ref, const char *anno_id)
{
    json_t *label = json_object_object_get(ref, "sortLabel");

    if (!label)
        label = json_object_object_get(ref, "displayLabel");

    if (label)
        add_unique("persons", json_object_get_string(label));
    else
        fprintf(stderr, "Missing 'sortLabel' and 'displayLabel' in %s\n", anno_id);
}

static void
index_person(json_t *body, const char *anno_id)
{
    json_t *ref;

    if (json_object_object_get_ex(body, "tei:ref", &ref)) {
        if (json_object_get_type(ref) == json_type_array) {
            int nitems = json_object_array_length(ref);
            for (int i = 0; i < nitems; i++)
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
    if (asprintf(&filename, "work/%s.txt", basename) != -1) {
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
    json_t *texts = get_obj(text_type);
    if (!texts) {
        texts = json_object_new_array();
        add_obj(text_type, texts);
    }

    json_t *targets = json_object_object_get(anno, "target");
    if (targets && json_object_get_type(targets) == json_type_array) {
        int nitems = json_object_array_length(targets);
        for (int i = 0; i < nitems; i++) {
            json_t *target = json_object_array_get_idx(targets, i);
            json_t *type = json_object_object_get(target, "type");
            if (type && streq("NormalText", json_object_get_string(type))) {
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
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <file>\n", argv[0]);
        return 1;
    }

    // setup locale for unicode reading
    setlocale(LC_ALL, "en_US.UTF-8");

    //fprintf(stderr, "Indexing %s\n", argv[1]);

    FILE *fp = fopen(argv[1], "r");
    if (!fp) {
        perror(argv[1]);
        return 1;
    }

    // allocate global (static) indexing doc
    _root = json_object_new_object();

    char *line = NULL;
    size_t linecap = 0;
    for (int line_num = 1; getline(&line, &linecap, fp) != -1; line_num++) {
        // each line in jsonld file is one web annotation
        json_t *anno = json_tokener_parse(line);
        if (!anno) {
            fprintf(stderr, "Error parsing JSON on line %d\n", line_num);
            continue;
        }

        json_t *id, *body, *type;
        if (json_object_object_get_ex(anno, "id", &id)
                && json_object_object_get_ex(anno, "body", &body)
                && json_object_object_get_ex(body, "type", &type)) {

            const char *anno_id = json_object_get_string(id);
            const char *body_type = json_object_get_string(type);

            switch (body_type[0]) {
                case 'D':
                    if (streq(body_type, "Document")) {
                        add_str("type", "intro");
                        index_anno(body);
                    }
                    else if (streq(body_type, "Division")) {
                        json_t *tei_type = json_object_object_get(body, "tei:type");
                        if (tei_type) {
                            const char *s = json_object_get_string(tei_type);
                            if (streq(s, "original"))
                                store_text(anno, "letterOriginalText");
                            else if (streq(s, "translation"))
                                store_text(anno, "letterTranslatedText");
                            else if (streq(s, "about"))
                                store_text(anno, "introText");
                        }
                    }
                    break;

                case 'E':
                    if (streq(body_type, "Entity")) {
                        json_t *tei_type = json_object_object_get(body, "tei:type");
                        if (tei_type) {
                            const char *s = json_object_get_string(tei_type);
                            if (streq(s, "artwork"))
                                index_artwork(body);
                            else if (streq(s, "person"))
                                index_person(body, anno_id);
                        }
                    }
                    break;

                case 'L':
                    if (streq(body_type, "Letter")) {
                        add_str("type", "letter");
                        index_anno(body);
                    }
                    break;

                case 'N':
                    if (streq(body_type, "Note")) {
                        json_t *subtype = json_object_object_get(body, "subtype");
                        if (subtype) {
                            const char *s = json_object_get_string(subtype);
                            if (streq(s, "notes")
                                    || streq(s, "typednotes")
                                    || streq(s, "langnotes"))
                                store_text(anno, "letterNotesText");
                        }
                    }
                    break;

                default:
                    // ignore
                    break;
            }
        }

        // dereference/free this iteration's anno
        json_object_put(anno);
    }

    // cleanup technically not needed as we're about to exit, but oh well.
    free(line);
    linecap = 0;
    fclose(fp);

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
