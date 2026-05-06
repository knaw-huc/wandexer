use serde_json::{json, Value};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::Path;

struct Indexer {
    root: Value,
}

impl Indexer {
    fn new() -> Self {
        Self {
            root: json!({}),
        }
    }

    /// Replaces find_by_path. Uses JSON pointer-like logic.
    fn find_by_path<'a>(&'a self, obj: &'a Value, path: &str) -> Option<&'a Value> {
        let mut current = obj;
        for token in path.split('.') {
            current = current.get(token)?;
        }
        Some(current)
    }

    fn add_str(&mut self, key: &str, val: &str) {
        self.root[key] = json!(val);
    }

    fn contrive_date(&mut self, body: &Value) -> Option<Value> {
        let actual = body.get("dateSent");
        let not_before = body.get("dateSentNotBefore");
        let not_after = body.get("dateSentNotAfter");
        let id = body.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");

        let mut date_obj = serde_json::Map::new();

        if let Some(act) = actual {
            date_obj.insert("gte".to_string(), act.clone());
            date_obj.insert("lte".to_string(), act.clone());
            if not_before.is_some() {
                eprintln!("{}: has both actual date AND notBefore!", id);
            }
            if not_after.is_some() {
                eprintln!("{}: has both actual date AND notAfter!", id);
            }
        } else {
            if let Some(nb) = not_before {
                date_obj.insert("gte".to_string(), nb.clone());
            }
            if let Some(na) = not_after {
                date_obj.insert("lte".to_string(), na.clone());
            }
        }

        if !date_obj.is_empty() {
            Some(Value::Object(date_obj))
        } else {
            None
        }
    }

    fn index_anno(&mut self, body: &Value) {
        let fields = [
            ("recipient", "recipient"),
            ("file", "n"),
            ("location", "location"),
            ("msId", "identifier"),
            ("sender", "sender"),
            ("title", "title"),
            ("correspondent", "participant"),
            ("period", "namedPeriod"),
        ];

        if let Some(id) = body.get("id").and_then(|v| v.as_str()) {
            self.add_str("id", id);
        }

        for (key, path) in fields {
            if let Some(field_val) = body.get(path) {
                self.root[key] = field_val.clone();
            }
        }

        if let Some(date) = self.contrive_date(body) {
            self.root["date"] = date.clone();
            if let Some(gte) = date.get("gte") {
                self.root["dateSortable"] = gte.clone();
            } else if let Some(lte) = date.get("lte") {
                self.root["dateSortable"] = lte.clone();
            }
        } else {
            self.root["date"] = json!({"gte": "0001", "lte": "9999"});
            self.root["dateSortable"] = json!("9999");
            eprintln!("{}: no dateSent, winging it.",
                body.get("id").and_then(|v| v.as_str()).unwrap_or("unknown"));
        }
    }

    fn add_unique(&mut self, key: &str, candidate: &str) {
        let arr = self.root.as_object_mut().unwrap()
            .entry(key)
            .or_insert(json!([]))
            .as_array_mut()
            .unwrap();

        if !arr.iter().any(|v| v.as_str() == Some(candidate)) {
            arr.push(json!(candidate));
        }
    }

    fn index_artwork(&mut self, body: &Value) {
        if let Some(ref_obj) = body.get("tei:ref") {
            if let Some(id) = ref_obj.get("id").and_then(|v| v.as_str()) {
                // We convert to String to "break" the borrow from self
                let id_owned = id.to_string(); 
                self.add_unique("artworkIds", &id_owned);
            }
            
            if let Some(label) = self.find_by_path(ref_obj, "label.en.search").and_then(|v| v.as_str()) {
                let label_owned = label.to_string();
                self.add_unique("artworksEN", &label_owned);
            }
        }
    }

    fn index_person_ref(&mut self, person_ref: &Value, anno_id: &str) {
        let label = person_ref.get("sortLabel")
            .or_else(|| person_ref.get("displayLabel"))
            .and_then(|v| v.as_str());

        match label {
            Some(l) => self.add_unique("persons", l),
            None => eprintln!("Missing 'sortLabel' and 'displayLabel' in {}", anno_id),
        }
    }

    fn index_person(&mut self, body: &Value, anno_id: &str) {
        if let Some(tei_ref) = body.get("tei:ref") {
            if let Some(arr) = tei_ref.as_array() {
                for item in arr {
                    self.index_person_ref(item, anno_id);
                }
            }
            else {
                self.index_person_ref(tei_ref, anno_id);
            }
        }
    }

    fn extract_text(&self, source: &str, start: usize, end: usize) -> Value {
        let filename = Path::new(source)
            .file_name()
            .and_then(|os_str| os_str.to_str())
            .map(|name| format!("work/{}.txt", name));

        if let Some(path) = filename {
            // Rust strings are UTF-8 by default. We read the file and use 
            // char-based slicing to mimic Unicode codepoint indexing.
            match fs::read_to_string(&path) {
                Ok(content) => {
                    let text: String = content.chars().skip(start).take(end - start).collect();
                    return json!(text);
                }
                Err(e) => {
                    let err_msg = format!("{}[{}..{}]: {}", path, start, end, e);
                    eprintln!("{}", err_msg);
                    return json!(err_msg);
                }
            }
        }
        json!("File path error")
    }

    fn store_text(&mut self, anno: &Value, text_type: &str) {
        let mut extracted_texts = Vec::new();

        if let Some(targets) = anno.get("target").and_then(|v| v.as_array()) {
            for target in targets {
                let is_normal = target.get("type").and_then(|v| v.as_str()) == Some("NormalText");
                if is_normal {
                    let source = target.get("source").and_then(|v| v.as_str());
                    let selector = target.get("selector");
                    let start = selector.and_then(|s| s.get("start")).and_then(|v| v.as_u64());
                    let end = selector.and_then(|s| s.get("end")).and_then(|v| v.as_u64());

                    if let (Some(src), Some(s), Some(e)) = (source, start, end) {
                        extracted_texts.push(self.extract_text(src, s as usize, e as usize));
                    }
                }
            }
        }

        if !extracted_texts.is_empty() {
            let arr = self.root.as_object_mut().unwrap()
                .entry(text_type)
                .or_insert(json!([]))
                .as_array_mut()
                .unwrap();
            arr.extend(extracted_texts);
        }
    }

    fn sort_fields(&mut self) {
        let sortable_fields = ["artworkIds", "artworksEN", "persons"];
        if let Some(obj) = self.root.as_object_mut() {
            for field in sortable_fields {
                if let Some(Value::Array(arr)) = obj.get_mut(field) {
                    arr.sort_by(|a, b| {
                        a.as_str().unwrap_or("").cmp(b.as_str().unwrap_or(""))
                    });
                }
            }
        }
    }
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <file>", args[0]);
        std::process::exit(1);
    }

    let file = File::open(&args[1])?;
    let reader = BufReader::new(file);
    let mut indexer = Indexer::new();

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = line_result?;
        let anno: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                eprintln!("Error parsing JSON on line {}", line_num + 1);
                continue;
            }
        };

        if let (Some(id), Some(body)) = (anno.get("id"), anno.get("body")) {
            let anno_id = id.as_str().unwrap_or("");
            let body_type = body.get("type").and_then(|v| v.as_str()).unwrap_or("");

        //let id = anno.get("id").and_then(|v| v.as_str());
        //let body = anno.get("body");
        //let body_type = body.and_then(|b| b.get("type")).and_then(|v| v.as_str());

        //if let (Some(anno_id), Some(body), Some(bt)) = (id, body, body_type) {
            match body_type {
                "Document" => {
                    indexer.add_str("type", "intro");
                    indexer.index_anno(body);
                }
                "Division" => {
                    if let Some(tei_type) = body.get("tei:type").and_then(|v| v.as_str()) {
                        match tei_type {
                            "original" => indexer.store_text(&anno, "letterOriginalText"),
                            "translation" => indexer.store_text(&anno, "letterTranslatedText"),
                            "about" => indexer.store_text(&anno, "introText"),
                            _ => {}
                        }
                    }
                }
                "Entity" => {
                    if let Some(tei_type) = body.get("tei:type").and_then(|v| v.as_str()) {
                        match tei_type {
                            "artwork" => indexer.index_artwork(body),
                            "person" => indexer.index_person(body, anno_id),
                            _ => {}
                        }
                    }
                }
                "Letter" => {
                    indexer.add_str("type", "letter");
                    indexer.index_anno(body);
                }
                "Note" => {
                    if let Some(subtype) = body.get("subtype").and_then(|v| v.as_str()) {
                        if matches!(subtype, "notes" | "typednotes" | "langnotes") {
                            indexer.store_text(&anno, "letterNotesText");
                        }
                    }
                }
                _ => {}
            }
        }
    }

    indexer.sort_fields();
    println!("{}", serde_json::to_string(&indexer.root).unwrap());

    Ok(())
}
