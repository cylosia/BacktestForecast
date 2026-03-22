import type { TemplateListResponse, TemplateResponse } from "@backtestforecast/api-client";
import { isValidTemplateConfig } from "@/lib/templates/parse";

function isRecord(value: unknown): value is Record<string, unknown> {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

export function validateTemplateResponse(data: unknown): TemplateResponse {
  if (!isRecord(data)) {
    throw new Error("response is not an object");
  }
  if (typeof data.id !== "string") throw new Error("template.id must be a string");
  if (typeof data.name !== "string") throw new Error("template.name must be a string");
  if (typeof data.strategy_type !== "string") throw new Error("template.strategy_type must be a string");
  if (typeof data.created_at !== "string") throw new Error("template.created_at must be a string");
  if (typeof data.updated_at !== "string") throw new Error("template.updated_at must be a string");
  if (!(data.description == null || typeof data.description === "string")) {
    throw new Error("template.description must be null or a string");
  }
  if (!isValidTemplateConfig(data.config)) {
    throw new Error("template.config does not satisfy TemplateConfig");
  }
  return data as TemplateResponse;
}

export function validateTemplateListResponse(data: unknown): TemplateListResponse {
  if (!isRecord(data)) {
    throw new Error("response is not an object");
  }
  if (!Array.isArray(data.items)) {
    throw new Error("items must be an array");
  }
  if (typeof data.total !== "number") {
    throw new Error("total must be a number");
  }
  if (!(data.template_limit == null || typeof data.template_limit === "number")) {
    throw new Error("template_limit must be null or a number");
  }
  data.items.forEach((item, index) => {
    try {
      validateTemplateResponse(item);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`items[${index}] invalid: ${message}`);
    }
  });
  return data as TemplateListResponse;
}
