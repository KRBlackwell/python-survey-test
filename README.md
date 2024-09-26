# python-survey-test
Test performance of some basic survey data operations and reading in data.
## performance.py
A wrapper for functions to show memory usage and time to run.

# Survey edit logic
The survey I work on primarily is very large, with a lot of rows and almost as many columns of data. There are over 200 edit programs, even more specs, and each edit ranges from 100-40k lines of code. The majority have about 10k lines of code. The edits are very complex, reading in data from various sources, including the main file.\
 \
In SAS, programmers would write code in an arrow pattern, with nested ifs. They are very good at this and are quickly able to add and make changes. There are still errors, but I am always impressed by their proficiency working this way. In Python, the go-to for a lot of staff has been line after line of transformations. I'm developing a way to demonstrate how they can organize that code. I'm pushing for many smaller methods, to facilitate unit testing, but that's been difficult to sell. I believe it will be adopted in time, and there is time to adopt it, but I'm working on a different approach that might be better received.

## Flattening conditionals
See below, D1 and D2 are where we check a different condition in each box. Set1 is where we set that group of rows to some value where D1 and D2 are true.
I have flattened conditionals in some programs, but the issue with that is we have very long and involved paths. It's easy to miss one or to lose track.
```
if ubornus = 1 then do;
  if state in (&states) then do;
    ebornus = ubornus;
    abornus = &flag.;
    ebornus_track = "Set1"
  end;
end;
```
becomes
```
if ubornus = 1 & state in (&states) then do;
  ebornus = ubornus;
  abornus = &flag.;
  ebornus_track = "Set1"
end;
```
This is simple enough with 2 conditionals, but add in 8 more, with several other paths that can happen, and you will have (logic path A) | (logic path B) | (logic path C) and it becomes easy to miss things, easy to make a mistake, and difficult to make changes.

## Tracking state
 ```mermaid
flowchart LR
    D1["`D1`"]
    D2["`D2`"]
    Set1["`Set1`"]
    D1 --> D2 --> Set1
```
One approach is based on the fact that the specs are flow charts, one box points to another. We can only get to D2 if D1 is true, and we can only get to Set1 if D2 is true. We don't have to check D1 there. As an example, if we have data stored in dataclasses:
```
 def d1(person, state):
   if in_universe:
     state.state = "d1"
     state.condition = person.ubornus == 1
```
Doing this particular implementation wasn't well received, but I believe it can be helpful, because coding an edit this way was much easier and faster for me. I was able to identify mistakes quickly through unit testing, and I did not lose track of if statements or complex flattened logic paths. I believe pairing this concept with groups of transformations might be an alternative that helps the team going forward.

## Chain of Responsibility
I still think this could be helpful, but I'm not convinced that a proper implementation of this pattern would work well with the way work is divided.

## Rules Engine
When I first arrived and began working on this particular survey, I thought a rules engine would be great. I'm still thinking it would be nice. The analysts like the idea, because they would have more transparency on how the spec is implemented in code.
