import { describe, expect, it } from "vitest";
import { mapTemplateFieldErrors } from "@/lib/templates/validation";

describe("mapTemplateFieldErrors", () => {
  it("maps top-level template field errors", () => {
    expect(
      mapTemplateFieldErrors([
        { loc: ["body", "name"], msg: "Name is required." },
        { loc: ["body", "description"], msg: "Description too long." },
      ]),
    ).toEqual({
      name: "Name is required.",
      description: "Description too long.",
    });
  });

  it("maps nested config field errors to defaultSymbol", () => {
    expect(
      mapTemplateFieldErrors([
        { loc: ["body", "config", "default_symbol"], msg: "Ticker is invalid." },
      ]),
    ).toEqual({
      defaultSymbol: "Ticker is invalid.",
    });
  });
});
