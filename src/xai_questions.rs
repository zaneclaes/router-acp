//! Translate Grok's `_x.ai/ask_user_question` extension into ACP elicitation.
//!
//! Grok's CLI emits that vendor method instead of `elicitation/create`. Clients
//! that only implement the ACP form (Kory Code's relay, goose, …) reject the
//! unknown method, so the question never becomes a card. When the upstream
//! client advertises form elicitation, the router maps the request to a form
//! and maps the answer back into Grok's `{outcome, answers}` shape.

use agent_client_protocol::schema::v1::{
    CreateElicitationRequest, CreateElicitationResponse, ElicitationAcceptAction,
    ElicitationAction, ElicitationContentValue, ElicitationFormMode, ElicitationSchema,
    ElicitationSessionScope, EnumOption, MultiSelectPropertySchema, StringPropertySchema,
};
use serde_json::{Value, json};

/// One Grok `ask_user_question` option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct XaiOption {
    pub label: String,
    pub description: Option<String>,
}

/// One Grok `ask_user_question` item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct XaiQuestion {
    pub question: String,
    pub options: Vec<XaiOption>,
    pub multi_select: bool,
}

/// Parsed `_x.ai/ask_user_question` params. Unparseable frames are forwarded raw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct XaiAskRequest {
    pub tool_call_id: Option<String>,
    pub questions: Vec<XaiQuestion>,
}

/// Parse Grok's vendor question frame. Returns `None` when the params are not
/// a usable question list so the caller can fall back to raw forwarding.
pub(crate) fn parse_request(params: &Value) -> Option<XaiAskRequest> {
    let questions = params.get("questions")?.as_array()?;
    let parsed: Vec<XaiQuestion> = questions.iter().filter_map(parse_question).collect();
    if parsed.is_empty() {
        return None;
    }
    let tool_call_id = params
        .get("toolCallId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Some(XaiAskRequest {
        tool_call_id,
        questions: parsed,
    })
}

fn parse_question(value: &Value) -> Option<XaiQuestion> {
    let question = value
        .get("question")
        .and_then(Value::as_str)
        .or_else(|| value.get("header").and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())?
        .to_string();
    let options = value
        .get("options")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().filter_map(parse_option).collect())
        .unwrap_or_default();
    let multi_select = value
        .get("multiSelect")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Some(XaiQuestion {
        question,
        options,
        multi_select,
    })
}

fn parse_option(value: &Value) -> Option<XaiOption> {
    let label = value
        .get("label")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())?
        .to_string();
    let description = value
        .get("description")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Some(XaiOption { label, description })
}

/// Build an ACP form elicitation under the *router* session id.
pub(crate) fn to_elicitation(req: &XaiAskRequest, router_sid: &str) -> CreateElicitationRequest {
    let mut scope = ElicitationSessionScope::new(router_sid.to_string());
    if let Some(tool_call_id) = &req.tool_call_id {
        scope = scope.tool_call_id(tool_call_id.as_str());
    }
    let mut schema = ElicitationSchema::new();
    for (i, question) in req.questions.iter().enumerate() {
        let key = format!("question_{i}");
        let custom_key = format!("{key}_custom");
        let options: Vec<EnumOption> = question
            .options
            .iter()
            .map(|opt| {
                let mut option = EnumOption::new(opt.label.clone(), opt.label.clone());
                if let Some(description) = &opt.description {
                    option = option.description(description.clone());
                }
                option
            })
            .collect();
        if question.multi_select {
            schema = schema.property(
                &key,
                MultiSelectPropertySchema::titled(options).title(question.question.clone()),
                false,
            );
        } else {
            let mut field = StringPropertySchema::new().title(question.question.clone());
            if !options.is_empty() {
                field = field.one_of(options);
            }
            schema = schema.property(&key, field, false);
        }
        schema = schema.property(&custom_key, custom_answer_field(&key), false);
    }
    let message = req
        .questions
        .iter()
        .map(|q| q.question.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    CreateElicitationRequest::new(ElicitationFormMode::new(scope, schema), message)
}

fn custom_answer_field(question_id: &str) -> StringPropertySchema {
    let mut meta = serde_json::Map::new();
    meta.insert(
        "_askUserQuestionCustomAnswer".to_string(),
        json!({
            "questionId": question_id,
            "isCustomAnswer": true,
        }),
    );
    StringPropertySchema::new().title("Other").meta(Some(meta))
}

/// Map an elicitation result into Grok's `{outcome, answers}` tool result.
///
/// Accept with a picked option (or custom text when no option is picked)
/// becomes `{outcome:"accepted", answers:{<question>:[labels…]}}`. Decline,
/// cancel, and unknown actions become `{outcome:"cancelled"}`.
pub(crate) fn from_elicitation(
    resp: &CreateElicitationResponse,
    questions: &[XaiQuestion],
) -> Value {
    match &resp.action {
        ElicitationAction::Accept(accept) => accepted_answers(accept, questions),
        _ => json!({ "outcome": "cancelled" }),
    }
}

fn accepted_answers(accept: &ElicitationAcceptAction, questions: &[XaiQuestion]) -> Value {
    let content = accept.content.as_ref();
    let mut answers = serde_json::Map::new();
    for (i, question) in questions.iter().enumerate() {
        let key = format!("question_{i}");
        let custom_key = format!("{key}_custom");
        let picked = content
            .and_then(|c| c.get(&key))
            .map(content_strings)
            .unwrap_or_default();
        let custom = content
            .and_then(|c| c.get(&custom_key))
            .map(content_strings)
            .unwrap_or_default();
        let labels = if picked.is_empty() { custom } else { picked };
        answers.insert(question.question.clone(), json!(labels));
    }
    json!({ "outcome": "accepted", "answers": answers })
}

fn content_strings(value: &ElicitationContentValue) -> Vec<String> {
    match value {
        ElicitationContentValue::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Vec::new()
            } else {
                vec![s.clone()]
            }
        }
        ElicitationContentValue::StringArray(values) => values
            .iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use agent_client_protocol::schema::v1::ElicitationMode;

    fn probed_single_select() -> Value {
        json!({
            "sessionId": "grok-sid",
            "toolCallId": "call_color",
            "questions": [{
                "question": "Which color?",
                "options": [
                    {"label": "Red", "description": "warm"},
                    {"label": "Blue", "description": "cool"}
                ],
                "multiSelect": false
            }],
            "mode": "default"
        })
    }

    #[test]
    fn parse_probed_single_select_frame() {
        let parsed = parse_request(&probed_single_select()).expect("parse");
        assert_eq!(parsed.tool_call_id.as_deref(), Some("call_color"));
        assert_eq!(parsed.questions.len(), 1);
        assert_eq!(parsed.questions[0].question, "Which color?");
        assert!(!parsed.questions[0].multi_select);
        assert_eq!(parsed.questions[0].options[0].label, "Red");
        assert_eq!(
            parsed.questions[0].options[0].description.as_deref(),
            Some("warm")
        );
    }

    #[test]
    fn unparseable_params_fall_back() {
        assert!(parse_request(&json!({})).is_none());
        assert!(parse_request(&json!({"questions": []})).is_none());
        assert!(parse_request(&json!({"questions": [{}]})).is_none());
        assert!(parse_request(&json!({"questions": "nope"})).is_none());
    }

    #[test]
    fn header_is_accepted_as_question_text() {
        let parsed = parse_request(&json!({
            "questions": [{"header": "Color", "options": [{"label": "Red"}]}]
        }))
        .expect("parse");
        assert_eq!(parsed.questions[0].question, "Color");
    }

    #[test]
    fn to_elicitation_uses_router_sid_and_form_shape() {
        let parsed = parse_request(&probed_single_select()).unwrap();
        let req = to_elicitation(&parsed, "router-sid");
        let value = serde_json::to_value(&req).unwrap();
        assert_eq!(value["mode"], "form");
        assert_eq!(value["sessionId"], "router-sid");
        assert_eq!(value["toolCallId"], "call_color");
        assert_eq!(value["message"], "Which color?");
        let props = &value["requestedSchema"]["properties"];
        assert_eq!(props["question_0"]["type"], "string");
        assert_eq!(props["question_0"]["title"], "Which color?");
        assert_eq!(props["question_0"]["oneOf"][0]["const"], "Red");
        assert_eq!(props["question_0"]["oneOf"][0]["title"], "Red");
        assert_eq!(props["question_0"]["oneOf"][0]["description"], "warm");
        assert_eq!(props["question_0_custom"]["type"], "string");
        assert_eq!(props["question_0_custom"]["title"], "Other");
        assert_eq!(
            props["question_0_custom"]["_meta"]["_askUserQuestionCustomAnswer"],
            json!({"questionId": "question_0", "isCustomAnswer": true})
        );
        assert!(matches!(req.mode, ElicitationMode::Form(_)));
    }

    #[test]
    fn multi_select_uses_array_any_of() {
        let parsed = parse_request(&json!({
            "questions": [{
                "question": "Pick toppings",
                "options": [{"label": "A"}, {"label": "B"}],
                "multiSelect": true
            }]
        }))
        .unwrap();
        let value = serde_json::to_value(to_elicitation(&parsed, "sid")).unwrap();
        let field = &value["requestedSchema"]["properties"]["question_0"];
        assert_eq!(field["type"], "array");
        assert_eq!(field["items"]["anyOf"][0]["const"], "A");
        assert_eq!(field["items"]["anyOf"][1]["const"], "B");
    }

    fn accept_content(pairs: &[(&str, ElicitationContentValue)]) -> CreateElicitationResponse {
        let mut content = BTreeMap::new();
        for (k, v) in pairs {
            content.insert((*k).to_string(), v.clone());
        }
        CreateElicitationResponse::new(ElicitationAction::Accept(
            ElicitationAcceptAction::new().content(Some(content)),
        ))
    }

    #[test]
    fn accept_option_maps_to_grok_answers() {
        let parsed = parse_request(&probed_single_select()).unwrap();
        let resp = accept_content(&[("question_0", ElicitationContentValue::from("Red"))]);
        assert_eq!(
            from_elicitation(&resp, &parsed.questions),
            json!({
                "outcome": "accepted",
                "answers": { "Which color?": ["Red"] }
            })
        );
    }

    #[test]
    fn accept_custom_text_when_no_option_picked() {
        let parsed = parse_request(&probed_single_select()).unwrap();
        let resp = accept_content(&[("question_0_custom", ElicitationContentValue::from("Green"))]);
        assert_eq!(
            from_elicitation(&resp, &parsed.questions),
            json!({
                "outcome": "accepted",
                "answers": { "Which color?": ["Green"] }
            })
        );
    }

    #[test]
    fn option_wins_over_custom_text() {
        let parsed = parse_request(&probed_single_select()).unwrap();
        let resp = accept_content(&[
            ("question_0", ElicitationContentValue::from("Blue")),
            ("question_0_custom", ElicitationContentValue::from("Green")),
        ]);
        assert_eq!(
            from_elicitation(&resp, &parsed.questions)["answers"]["Which color?"],
            json!(["Blue"])
        );
    }

    #[test]
    fn multi_select_accept_returns_label_array() {
        let parsed = parse_request(&json!({
            "questions": [{
                "question": "Pick toppings",
                "options": [{"label": "A"}, {"label": "B"}],
                "multiSelect": true
            }]
        }))
        .unwrap();
        let resp = accept_content(&[("question_0", ElicitationContentValue::from(vec!["A", "B"]))]);
        assert_eq!(
            from_elicitation(&resp, &parsed.questions),
            json!({
                "outcome": "accepted",
                "answers": { "Pick toppings": ["A", "B"] }
            })
        );
    }

    #[test]
    fn decline_and_cancel_are_cancelled() {
        let parsed = parse_request(&probed_single_select()).unwrap();
        for action in [ElicitationAction::Decline, ElicitationAction::Cancel] {
            let resp = CreateElicitationResponse::new(action);
            assert_eq!(
                from_elicitation(&resp, &parsed.questions),
                json!({ "outcome": "cancelled" })
            );
        }
    }
}
