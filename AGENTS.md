# AGENTS.md

Instructions for a coding agent, or a person, changing this repository.

qedro-lab runs every place qedro reads OpenLineage from on one machine, with real
emitters writing to all of them, so qedro's readers are tested against real systems
and its fixtures can be captured again. It holds fictitious data only.

## How a change gets in

- **Fork, branch, open a pull request against `main`.** Do not push to `main`.
- **The check has to pass** on a clean stack before and after the change:

  ```bash
  docker compose down -v && docker compose up -d --wait
  docker compose run --rm lab day 1 && docker compose run --rm lab day 2
  docker compose run --rm lab check
  ```

- **Sign off every commit** with `git commit -s`, under the
  [Developer Certificate of Origin](https://developercertificate.org), and sign it with
  an SSH or GPG key.
- **Agents are welcome, and saying so is welcome too, not required.** The person
  opening the pull request is responsible for every line and has read every one.

## Ask before changing these

- **A version in `compose.yaml` or `lab/requirements.txt`.** A capture names the
  versions that produced it; a bump changes what every later capture means, so it is
  deliberate and said in the pull request.
- **`lab/openlineage.yml`.** It is the configuration qedro's pilot guide recommends;
  a change here should be one that guide would make too.
- **`lab/generate.py`.** The same day must give the same rows, or two captures of one
  day stop being comparable.

## Never

- **Never use real data or real credentials.** Names are ACME's specimen names,
  addresses use `.example`, and every credential is an emulator's published default.
- **Never expose a port beyond `127.0.0.1`.** The emulators accept those published
  credentials from anyone who can reach them.
- **Never weaken `check` to make it pass.** A source that differs from the directory —
  missing events, extra ones, or one stored twice — is a finding, and the fix goes
  into the stack's configuration or into qedro, with the reason written beside it.
- **Never pin `:latest`.** Every image and package names its version.

## How to work here

- **Write the cause beside the workaround.** Each setting that exists because something
  misbehaved (`GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT`, the Marquez timeout) says what
  happened and how it was measured, and the README's findings list carries it too.
- **Check a name against its source** — the OpenLineage client's own config classes,
  an image's own config file — rather than writing the one it seems it should have.
