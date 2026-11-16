import { useState } from "react";
import { Button } from "@datadog/druids/form/Button";
import { RadioButtons } from "@datadog/druids/form/RadioButtons";
import { StatusPill } from "@datadog/druids/pills/StatusPill";
import { Text } from "@datadog/druids/typography/Text";
import type { Question } from "../data/personas";
import { useProgress } from "../lib/progress";

// A short check at the end of a module. Getting every answer right marks the
// "check" lesson done.
export default function Quiz({ slug, questions }: { slug: string; questions: Question[] }) {
  const { setDone } = useProgress();
  const [answers, setAnswers] = useState<(number | undefined)[]>(questions.map(() => undefined));
  const [checked, setChecked] = useState(false);

  const allAnswered = answers.every((a) => a !== undefined);
  const score = answers.filter((a, i) => a === questions[i].answer).length;

  function check() {
    setChecked(true);
    if (score === questions.length) setDone(slug, "check", true);
  }

  return (
    <div className="quiz">
      {questions.map((q, i) => {
        const isRight = answers[i] === q.answer;
        return (
          <fieldset key={q.prompt} className="quiz-question">
            <legend>
              <Text weight="bold">
                {i + 1}. {q.prompt}
              </Text>
            </legend>
            <RadioButtons
              name={`${slug}-q${i}`}
              options={q.options.map((label, value) => ({ value, label }))}
              value={answers[i]}
              onChange={(value) => {
                setChecked(false);
                setAnswers((prev) => prev.map((a, j) => (j === i ? (value as number) : a)));
              }}
            />
            {checked && (
              <div className="quiz-feedback">
                <StatusPill level={isRight ? "success" : "danger"} isSoft>
                  {isRight ? "Correct" : "Not quite"}
                </StatusPill>
                <Text size="sm" variant="secondary">
                  {q.why}
                </Text>
              </div>
            )}
          </fieldset>
        );
      })}
      <div className="quiz-actions">
        <Button isPrimary isDisabled={!allAnswered} onClick={check} label="Check answers" isTitleCased={false} />
        {checked && (
          <Text size="sm" variant={score === questions.length ? "success" : "secondary"}>
            {score} of {questions.length} correct
            {score === questions.length ? " · lesson marked done" : ""}
          </Text>
        )}
      </div>
    </div>
  );
}
