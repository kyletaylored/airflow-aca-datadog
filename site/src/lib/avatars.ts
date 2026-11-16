// Illustrated faces, generated at build time from each persona's seed.
// DiceBear "Notionists" by Zoish, CC0 1.0 (https://heyzoish.gumroad.com/l/notionists).
import { createAvatar } from "@dicebear/core";
import * as notionists from "@dicebear/notionists";
import { ownerColors, type Owner } from "../data/pipeline";
import { personas } from "../data/personas";

export function avatarUri(slug: Owner): string {
  const persona = personas.find((p) => p.slug === slug)!;
  return createAvatar(notionists, {
    seed: persona.avatarSeed,
    backgroundColor: [ownerColors[slug].soft.replace("#", "")],
    radius: 50,
  }).toDataUri();
}

export function allAvatars(): Record<Owner, string> {
  return Object.fromEntries(personas.map((p) => [p.slug, avatarUri(p.slug)])) as Record<Owner, string>;
}
