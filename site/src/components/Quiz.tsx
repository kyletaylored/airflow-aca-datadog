import { useState } from "react";
import { Button } from "@datadog/druids/form/Button";
import { RadioButtons } from "@datadog/druids/form/RadioButtons";
import { StatusPill } from "@datadog/druids/pills/StatusPill";
import { Text } from "@datadog/druids/typography/Text";
import type { Question } from "../data/personas";
import { useProgress } from "../lib/progress";

// A short check at the end of a module, and its sign-off: getting every answer right marks the
// "check" lesson done, which completes the module.
export default function Quiz({ slug, questions, unit = "module" }: { slug: string; questions: Question[]; unit?: string }) {
  const { progress, setDone } = useProgress();
  const signedOff = progress[slug]?.includes("check") ?? false;
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
      <Text as="p" size="sm" variant="secondary">
        Answer every question correctly to sign off and complete the {unit}.
      </Text>
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
                  {isRight ? "Correct" : "Incorrect"}
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
          </Text>
        )}
      </div>
      {signedOff && (
        <div className="sign-off" role="status">
          <StatusPill level="success">Signed off</StatusPill>
          <Text size="sm">
            {checked && score === questions.length ? `You have completed this ${unit}.` : `You completed this ${unit} earlier. Your progress is saved in this browser.`}
          </Text>
        </div>
      )}
    </div>
  );
}
