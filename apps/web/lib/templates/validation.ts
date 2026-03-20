import type { ValidationFieldError } from "@/lib/api/shared";

export interface TemplateFieldErrors {
  name?: string;
  description?: string;
  defaultSymbol?: string;
}

const TEMPLATE_FIELD_NAME_MAP: Record<string, keyof TemplateFieldErrors> = {
  name: "name",
  description: "description",
  default_symbol: "defaultSymbol",
};

export function mapTemplateFieldErrors(fieldErrors: ValidationFieldError[] | undefined): TemplateFieldErrors {
  if (!fieldErrors || fieldErrors.length === 0) {
    return {};
  }

  const mapped: TemplateFieldErrors = {};
  for (const fieldError of fieldErrors) {
    const loc = fieldError.loc ?? [];
    const candidate = [...loc].reverse().find((part) => part !== "body" && part !== "config");
    if (!candidate) {
      continue;
    }
    const field = TEMPLATE_FIELD_NAME_MAP[candidate];
    if (!field || mapped[field]) {
      continue;
    }
    mapped[field] = fieldError.msg ?? "Invalid value.";
  }

  return mapped;
}
