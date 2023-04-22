# BSky Experiments

This repo has contains some fun Go experiments interacting with BlueSky via the AT Protocol.

The Makefile contains some useful targets for building and running the experiments.

## Experiments

### Mention Counter

The mention counter is a simple experiment that connects to the BlueSky Firehose and counts the number of mentions of users to see who is being harangued the most by the BlueSky community.


To run this experiment, copy the `.env.example` file to `.env` and add your `bsky` email and an app password.

```shell
$ cp .env.example .env
```

Then build and the docker image with the following make target:

```shell
$ make docker-run
```

#### Example Output

```shell
$ make docker-run
Building Docker image...
<...>
Running Docker container...
docker run --name bsky-mention-counts --rm --env-file .env -v /home/user/documents/ericvolp12/bsky-experiments/data/:/app/data/ bsky-mention-counts
[22.04.23 17:35:24] col.bsky.social: 
        @prer.at you have reached maximum free likes - any further likes cost a follow back
        Mentions: [@prer.at]
[22.04.23 17:35:24] thesecretsauce.bsky.social: 
        I made it on to @bsky.app thanks to @aliafonzy.bsky.social! Let’s see what’s going on in here… 👀 

        Hey ya’ll.
        Mentions: [@bsky.app @aliafonzy.bsky.social]
[22.04.23 17:35:54] writing mention counts to file...
^Cmake: *** [Makefile:29: docker-run] Error 2
$ # Read the output file
$ cat data/mention-counts.txt
prer.at: 1
bsky.app: 1
aliafonzy.bsky.social: 1
```
