/**
 * TS 5 standard decorators must work the same under the test transpiler and under `tsc`.
 *
 * Registration is a decorator in this port (Python's `__init_subclass__`), so a toolchain that
 * silently drops or mis-lowers a class decorator would not fail loudly — it would produce a
 * package whose registries are empty. This file is the canary: it decorates a class, and asserts
 * at runtime that the decorator ran, saw the class, and set a static field on it. The same file is
 * part of the `tsc --noEmit` surface, so both halves of the toolchain are covered.
 */

import { describe, expect, it } from 'vitest';

/** What the decorator writes onto the class it is applied to. */
interface Registered {
  registrationKey: string;
}

const definitionOrder: string[] = [];

/** A class decorator in the standard form: `(value, context) => void`. */
const registered = (key: string) => {
  return <T extends Registered>(target: T, context: ClassDecoratorContext): void => {
    definitionOrder.push(`${context.kind}:${String(context.name)}`);
    target.registrationKey = key;
  };
};

@registered('url')
class UrlDataPoint {
  public static registrationKey = '';
}

@registered('email')
class EmailDataPoint {
  public static registrationKey = '';

  public constructor(public readonly value: string) {}
}

describe('standard class decorators', () => {
  it('runs at class definition time and sees the class name', () => {
    expect(definitionOrder).toEqual(['class:UrlDataPoint', 'class:EmailDataPoint']);
  });

  it('sets a static field on the decorated class', () => {
    expect(UrlDataPoint.registrationKey).toBe('url');
    expect(EmailDataPoint.registrationKey).toBe('email');
  });

  it('leaves the class itself usable', () => {
    expect(new EmailDataPoint('a@example.com').value).toBe('a@example.com');
    expect(EmailDataPoint.name).toBe('EmailDataPoint');
  });
});
